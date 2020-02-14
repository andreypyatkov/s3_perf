
CXX=g++
CXXFLAGS=-std=c++14 -O3
LDLIBS=-lstdc++ -lpthread -lgflags -laws-cpp-sdk-core -laws-cpp-sdk-s3

s3_perf: s3_perf.cc
	$(CXX) $(CXXFLAGS) s3_perf.cc -o s3_perf $(LDLIBS)

clean:
	rm -f ./s3_perf *.log
