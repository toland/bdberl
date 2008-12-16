require 'rake/clean'
require 'set'

PWD = Dir.getwd
BASE_DIR = File.expand_path "#{PWD}"

SRC = FileList['src/*.erl']
OBJ = SRC.pathmap("%{src,ebin}X.beam")
EBIN = FileList["#{BASE_DIR}/**/ebin"]
APP = FileList["#{PWD}/ebin/*.app"][0]

INCLUDE = "./include"
ERLC_FLAGS = "-I#{INCLUDE} -pa #{EBIN.join(' -pa ')} +debug_info "

CLEAN.include %w( **/*.beam **/erl_crash.dump )
CLOBBER.include %w( perf_test/logs int_test/logs test/logs doc )

directory 'ebin'

rule ".beam" => ["%{ebin,src}X.erl"] do |t|
  puts "compiling #{t.source}..."
  sh "erlc -W #{ERLC_FLAGS} -o ebin #{t.source}", :verbose => false
end

desc "Compile Erlang sources to .beam files"
task :compile => ['ebin'] + OBJ do
  do_validate_app
end

desc "Do a fresh build from scratch"
task :rebuild => [:clean, :compile]

task :default => [:compile]

task :compile_tests do
  do_compile_tests("test")
end

desc "Run unit tests"
task :test => [:compile, :compile_tests]

task :compile_int_tests do
  do_compile_tests("int_test")
end

desc "Run integration tests"
task :int_test => [:compile, :compile_int_tests]

def erl_run(script, args = "") 
  `erl -eval '#{script}' -s erlang halt #{args} -noshell 2>&1`.strip
end

def erl_where(lib)
  script = <<-ERL
      io:format("~s\n", [filename:join(code:lib_dir(#{lib}), "include")])
      ERL
  erl_run(script)
end

def erl_app_modules(app)
  script = <<-ERL
      ok = application:load(#{app}),
      {ok, M} = application:get_key(#{app}, modules),
      [io:format("~s\\n", [Mod]) || Mod <- M].
      ERL

  output = erl_run(script, "-pa ebin")
  if output[/badmatch/]
    fail "Error processing .app file: ", output
    ""
  else
    output.split("\n")
  end
end

def erl_app_version(app)
  script = <<-ERL
     ok = application:load(#{app}),
     {ok, Vsn} = application:get_key(#{app}, vsn),
     io:format("~s\\n", [Vsn]).
     ERL
  output = erl_run(script, "-pa ebin")
  output.strip()
end

def erl_app_version(app)
  script = <<-ERL
     ok = application:load(#{app}),
     {ok, Vsn} = application:get_key(#{app}, vsn),
     io:format("~s\\n", [Vsn]).
     ERL
  output = erl_run(script, "-pa ebin")
  output.strip()
end

def erts_dir()
  script = <<-ERL
      io:format("~s\n", [lists:concat([code:root_dir(), "/erts-", erlang:system_info(version)])])
      ERL
  erl_run(script)
end

def erts_link_cflags()
  if `uname`.strip() == "Darwin"
    " -fPIC -bundle -flat_namespace -undefined suppress "
  else
    " -fpic -shared"
  end
end

def do_validate_app()
  # Setup app name and build sets of modules from the .app as well as
  # beams that got compiled
  if APP == nil
    puts "No .app file found; skipping validation."
    return
  end

  app = File.basename(APP, ".app")
  modules = Set.new(erl_app_modules(app))
  beams = Set.new(OBJ.pathmap("%n").to_a)

  puts "validating #{app}.app..."

  # Identify .beam files which are listed in the .app, but not present in ebin/
  missing_beams = (modules - beams)
  if not missing_beams.empty?
    msg = "One or more modules listed in #{app}.app do not exist as .beam:\n"
    missing_beams.each { |m| msg << " * #{m}\n" }
    fail msg
  end

  # Identify modules which are not listed in the .app, but are present in ebin/
  missing_modules = (beams - modules)
  if not missing_modules.empty? 
    msg = "One or more .beam files exist that are not listed in #{app}.app:\n"
    missing_modules.each { |m| msg << "  * #{m}\n" }
    fail msg
  end
end


def do_compile_tests(dir)
  if File.directory?(dir)
    compile_cmd = "erlc #{ERLC_FLAGS} -I#{erl_where('common_test')} \
        -I#{erl_where('test_server')} -o #{dir} #{dir}/*.erl".squeeze(" ")

    sh compile_cmd, :verbose => false

    Dir.mkdir "#{dir}/logs" unless File.directory?("#{dir}/logs")
  end  
end

def run_tests(dir, rest = "")  
  sh "erl -pa #{EBIN.join(' ')} #{PWD}/ebin #{PWD}/include \
	            -noshell -s ct_run script_start -s erlang halt \
                    #{get_cover(dir)} \
	            #{get_suites(dir)} -logdir #{dir}/logs -env TEST_DIR #{PWD}/#{dir} \
	            #{rest}" , :verbose => false

end

def get_cover(dir)
  use_cover = ENV["use_cover"]
  if use_cover
    "-cover #{dir}/cover.spec"
  else
    ""
  end
end

def get_suites(dir)
  suites = ENV['suites']
  if suites
    all_suites = ""
    suites.each(' ') {|s| all_suites << "#{PWD}/#{dir}/#{s.strip}_SUITE "}
    "-suite #{all_suites}"
  else
    "-dir #{dir}"
  end
end
