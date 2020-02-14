/*
 * Copyright (c) 2020 Nutanix Inc. All rights reserved.
 *
 * Author: andrey.pyatkov@nutanix.com
 *
 * Simple S3 performance test.
 */

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <gflags/gflags.h>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <fstream>
#include <mutex>
#include <random>
#include <sys/stat.h>
#include <thread>
#include <vector>

DEFINE_string(bucket_name, "ltsstest",
              "S3 bucket name");

DEFINE_string(region, "us-west-1",
              "S3 bucket region");

DEFINE_string(prefix, "obj/",
              "Object name prefix. The final name is "
              "<prefix><thread_num>_<obj_num>");

DEFINE_int32(obj_size_kb, 1024,
             "Object size in kilobytes");

DEFINE_int32(num_threads, 1,
             "Number of upload threads");

DEFINE_int32(num_objects, 100,
             "Number of objects per thread");

DEFINE_int32(num_connections, 25,
             "Number of connections per thread");

DEFINE_int32(num_outstanding_req, 0,
             "Number of outstanding requests per thread. 0 makes it equal "
             "to num_connections");

DEFINE_string(stage, "all",
              "Defines the stages to test: 'upload', 'download', or 'all'");

DEFINE_int32(count, 5,
             "Number of times each stage should be executed");

//-----------------------------------------------------------------------------

using namespace google;
using namespace std;
using namespace std::chrono;

// Data chunk to upload.
static Aws::String g_obj;

//-----------------------------------------------------------------------------

static void InitChunk() {
  // Create a chunk filled with random numbers.
  random_device rd;
  mt19937 gen(rd());
  uniform_int_distribution<> dis(0, 255);

  g_obj.resize(FLAGS_obj_size_kb * 1024);
  for (int ii = 0; ii < FLAGS_obj_size_kb * 1024; ++ii) {
    g_obj[ii] = (char)dis(gen);
  }
}

static void PrintVars() {
  // Print all the gflags.
  cout << "Test configuration:" << endl;
  vector<CommandLineFlagInfo> vec;
  GetAllFlags(&vec);
  for (const auto& v : vec) {
    if (v.filename.find(__FILE__) != string::npos) {
      cout << "  " << v.name << " = " << v.current_value << " " << endl;
    }
  }
  cout << endl;
}

static Aws::String GetObjPrefix(int thread_num) {
  return Aws::String(FLAGS_prefix.c_str(), FLAGS_prefix.size()) +
    Aws::Utils::StringUtils::to_string(thread_num) + "_";
}

class ReportDuration {
 public:
  ReportDuration(const string& operation,
                 int num_threads,
                 int obj_per_thread,
                 int obj_size_kb)
  : operation_(operation), num_threads_(num_threads),
    obj_per_thread_(obj_per_thread), obj_size_kb_(obj_size_kb) {

    cout << operation << " starting" << endl;
    t0_ = high_resolution_clock::now();
  }

  ~ReportDuration() {
    high_resolution_clock::time_point t1 = high_resolution_clock::now();
    double time_sec = duration_cast<duration<double>>(t1 - t0_).count();
    int num_obj = num_threads_ * obj_per_thread_;
    double total_size_mb = (double)obj_size_kb_ * num_obj / 1024;
    cout << operation_ << " completed in " << time_sec << " seconds (total: "
         << num_obj << " objects, " << total_size_mb << " MB)" << endl
         << operation_ << " throughput: " << (total_size_mb / time_sec)
         << " MB/sec, " << (num_obj / time_sec) << " obj/sec" << endl << endl;
    fflush(stdout);
  }

 private:
  const string operation_;
  const int num_threads_, obj_per_thread_, obj_size_kb_;
  high_resolution_clock::time_point t0_;
};

class Ctx : public Aws::Client::AsyncCallerContext {
 public:
  void GetAvailableSlot() {
    unique_lock<mutex> lck(mtx_);
    if (num_outstanding_req_ >= FLAGS_num_outstanding_req) {
      // No slots available, wait for one.
      cond_.wait(lck);
    }
    assert(num_outstanding_req_ < FLAGS_num_outstanding_req);
    ++num_outstanding_req_;
  }

  void ReleaseSlot() const {
    unique_lock<mutex> lck(mtx_);
    assert(num_outstanding_req_ > 0);
    --num_outstanding_req_;
    cond_.notify_one();
  }

  void WaitAll() {
    // Wait for all outstanding requests to complete.
    unique_lock<mutex> lck(mtx_);
    while (num_outstanding_req_ > 0) {
      cond_.wait(lck);
    }
  }

 private:
  mutable mutex mtx_;
  mutable condition_variable cond_;
  mutable int num_outstanding_req_{0};
};

//-----------------------------------------------------------------------------
// Upload
//-----------------------------------------------------------------------------

static void ObjUploadDone(
  const Aws::S3::S3Client *client,
  const Aws::S3::Model::PutObjectRequest& request,
  const Aws::S3::Model::PutObjectOutcome& outcome,
  const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context) {

  if (!outcome.IsSuccess()) {
    auto error = outcome.GetError();
    cerr << "ERROR: " << error.GetExceptionName() << ": "
      << error.GetMessage() << endl;
    exit(1);
  }

  dynamic_pointer_cast<const Ctx>(context)->ReleaseSlot();
}

static void UploadThread(const int thread_num) {
  Aws::Client::ClientConfiguration clientConfig;
  //clientConfig.followRedirects = true;
  clientConfig.region = FLAGS_region.c_str();
  clientConfig.maxConnections = FLAGS_num_connections;

  Aws::S3::S3Client s3_client(clientConfig);
  const Aws::String s3_bucket_name = FLAGS_bucket_name.c_str();
  const Aws::String obj_name_prefix = GetObjPrefix(thread_num);

  shared_ptr<Aws::Client::AsyncCallerContext> aws_ctx = make_shared<Ctx>();
  Ctx& ctx = *dynamic_cast<Ctx *>(aws_ctx.get());

  // Upload objects.
  for (int ii = 0; ii < FLAGS_num_objects; ++ii) {
    Aws::S3::Model::PutObjectRequest object_request;
    shared_ptr<Aws::IOStream> input_data =
      Aws::MakeShared<Aws::StringStream>("TestTag", g_obj);

    object_request.SetBucket(s3_bucket_name);
    object_request.SetKey(
      obj_name_prefix + Aws::Utils::StringUtils::to_string(ii));

    object_request.SetBody(input_data);

    ctx.GetAvailableSlot();

    // Put the object.
    s3_client.PutObjectAsync(object_request, ObjUploadDone, aws_ctx);
  }

  ctx.WaitAll();
}

static void Upload(const int iteration) {
  vector<thread *> threads;
  ReportDuration report(string("  [") + to_string(iteration) + "] UPLOAD",
                        FLAGS_num_threads,
                        FLAGS_num_objects,
                        FLAGS_obj_size_kb);

  for (int ii = 0; ii < FLAGS_num_threads; ++ii) {
    threads.push_back(new thread(UploadThread, ii));
  }
  for (int ii = 0; ii < FLAGS_num_threads; ++ii) {
    threads[ii]->join();
    delete threads[ii];
  }
}

//-----------------------------------------------------------------------------
// Download
//-----------------------------------------------------------------------------

static void ObjDownloadDone(
  const Aws::S3::S3Client *client,
  const Aws::S3::Model::GetObjectRequest& request,
  const Aws::S3::Model::GetObjectOutcome& outcome,
  const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context) {

  if (!outcome.IsSuccess()) {
    auto error = outcome.GetError();
    cerr << "ERROR: " << error.GetExceptionName() << ": "
      << error.GetMessage() << endl;
    exit(1);
  }

  if (outcome.GetResult().GetContentLength() != FLAGS_obj_size_kb * 1024) {
    cerr << "ERROR: invalid object size "
         << outcome.GetResult().GetContentLength()
         << ", expected " << (FLAGS_obj_size_kb * 1024) << " bytes" << endl;
    exit(1);
  }

  dynamic_pointer_cast<const Ctx>(context)->ReleaseSlot();
}

static void DownloadThread(const int thread_num) {
  Aws::Client::ClientConfiguration clientConfig;
  //clientConfig.followRedirects = true;
  clientConfig.region = FLAGS_region.c_str();
  clientConfig.maxConnections = FLAGS_num_connections;

  Aws::S3::S3Client s3_client(clientConfig);
  const Aws::String s3_bucket_name = FLAGS_bucket_name.c_str();
  const Aws::String obj_name_prefix = GetObjPrefix(thread_num);

  shared_ptr<Aws::Client::AsyncCallerContext> aws_ctx = make_shared<Ctx>();
  Ctx& ctx = *dynamic_cast<Ctx *>(aws_ctx.get());

  // Upload objects.
  for (int ii = 0; ii < FLAGS_num_objects; ++ii) {
    Aws::S3::Model::GetObjectRequest object_request;
    object_request.SetBucket(s3_bucket_name);
    object_request.SetKey(
      obj_name_prefix + Aws::Utils::StringUtils::to_string(ii));

    ctx.GetAvailableSlot();

    // Put the object.
    s3_client.GetObjectAsync(object_request, ObjDownloadDone, aws_ctx);
  }

  ctx.WaitAll();
}

static void Download(const int iteration) {
  vector<thread *> threads;
  ReportDuration report(string("  [") + to_string(iteration) + "] DOWNLOAD",
                        FLAGS_num_threads,
                        FLAGS_num_objects,
                        FLAGS_obj_size_kb);

  for (int ii = 0; ii < FLAGS_num_threads; ++ii) {
    threads.push_back(new thread(DownloadThread, ii));
  }
  for (int ii = 0; ii < FLAGS_num_threads; ++ii) {
    threads[ii]->join();
    delete threads[ii];
  }
}

//-----------------------------------------------------------------------------

int main(int argc, char** argv) {
  ParseCommandLineFlags(&argc, &argv, false);
  if (FLAGS_num_outstanding_req <= 0) {
    FLAGS_num_outstanding_req = FLAGS_num_connections;
  }

  Aws::SDKOptions options;
  //options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Trace;
  Aws::InitAPI(options);

  PrintVars();

  if (FLAGS_stage != "download") {
    ReportDuration report("UPLOAD stage",
                          FLAGS_num_threads,
                          FLAGS_num_objects * FLAGS_count,
                          FLAGS_obj_size_kb);
    for (int ii = 1; ii <= FLAGS_count; ++ii) {
      InitChunk();
      Upload(ii);
    }
  }
  if (FLAGS_stage != "upload") {
    ReportDuration report("DOWNLOAD stage",
                          FLAGS_num_threads,
                          FLAGS_num_objects * FLAGS_count,
                          FLAGS_obj_size_kb);
    for (int ii = 1; ii <= FLAGS_count; ++ii) {
      Download(ii);
    }
  }

  Aws::ShutdownAPI(options);
  return 0;
}
