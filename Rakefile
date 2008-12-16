load "base.rake"

C_SRCS = FileList["c_src/*.c"]
C_OBJS = C_SRCS.pathmap("%X.o")

CLEAN.include %w( c_src/*.o priv/*.so  )
CLOBBER.include %w( c_src/system )

directory 'c_src'

DB_LIB = "c_src/system/lib/libdb.a"
DRIVER = "priv/bdberl_drv.so"

file DB_LIB do
  sh "cd c_src && ./buildlib.sh 2>&1"
end

file DRIVER => [:compile_c] do
  puts "linking priv/#{DRIVER}..."
  sh "gcc -g #{erts_link_cflags()} c_src/*.o c_src/system/lib/libdb-*.a -o #{DRIVER}", :verbose => false
end

rule ".o" => ["%X.c", "%X.h"] do |t|
  puts "compiling #{t.source}..."
  sh "gcc -g -c -Wall -Werror -fPIC #{dflag} -Ic_src/system/include -I#{erts_dir()}/include #{t.source} -o #{t.name}", :verbose => false
end

def dflag()
  ENV["release"] ? "" : "-DDEBUG"
end

task :compile_c => ['c_src'] + C_OBJS

task :compile => [DB_LIB, DRIVER]

task :test do
  run_tests "test", "+A10"
end

task :int_test do
  run_tests "int_test", "+A10"
end

# task :compile_perf_tests do
#   do_compile_tests("perftest")
# end
# 
# desc "Run performance tests"
# task :perftest => [:compile, :compile_perf_tests] do
#   run_tests "perftest", "+A10"
# end
