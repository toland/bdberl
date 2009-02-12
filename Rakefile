require 'erlbox'
require 'erlbox/driver'

CLOBBER.include %w( c_src/system )

UNIT_TEST_FLAGS << '+A10'
INT_TEST_FLAGS << '+A10'

DB_LIB = "c_src/system/lib/libdb.a"

file DB_LIB do
  sh "cd c_src && ./buildlib.sh 2>&1"
end

# This is a slightl kludgey way of getting DB_LIB into
# dependency chain early enough
file 'c_src/bdberl_drv.c' => [DB_LIB]

task :package do
  target_dir = package_dir
  Dir.mkdir "#{target_dir}/priv/bin"
  FileUtils.cp_r Dir.glob('c_src/system/bin/*'), "#{target_dir}/priv/bin", :verbose => false
end
