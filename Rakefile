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

task :package => [:compile] do
  app = File.basename(APP, ".app")
  vsn = erl_app_version(app)
  target_dir = "#{app}-#{vsn}"
  Dir.mkdir target_dir
  cp_r 'ebin', target_dir
  cp_r 'include', target_dir
  cp_r 'src', target_dir
  cp_r 'priv', target_dir
  Dir.mkdir "#{target_dir}/priv/bin"
  cp_r Dir.glob('c_src/system/bin/*'), "#{target_dir}/priv/bin"
  puts "Packaged to #{target_dir}"
end

