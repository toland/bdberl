require 'erlbox'
require 'erlbox/driver'
require 'erlbox/snmp'

CLOBBER.include %w( c_src/system )

UNIT_TEST_FLAGS << '+A10'
INT_TEST_FLAGS << '+A10'

DB_LIB = "c_src/system/lib/libdb.a"

CC_FLAGS << "-Ic_src/system/include"
LD_LIBS  << DB_LIB

file DB_LIB do
  sh "cd c_src && ./buildlib.sh 2>&1"
end

# This is a slightl kludgey way of getting DB_LIB into
# dependency chain early enough
file 'c_src/bdberl_drv.c' => [DB_LIB]

task 'faxien:package' do
  Dir.mkdir "#{PACKAGE_DIR}/priv/bin"
  FileUtils.cp_r Dir.glob('c_src/system/bin/*'), "#{PACKAGE_DIR}/priv/bin", :verbose => false
  FileUtils.cp_r Dir.glob('mibs/*.mib'), "#{PACKAGE_DIR}/mibs", :verbose => false
end
