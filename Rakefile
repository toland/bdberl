## -------------------------------------------------------------------
##
## Permission is hereby granted, free of charge, to any person obtaining a copy
## of this software and associated documentation files (the "Software"), to deal
## in the Software without restriction, including without limitation the rights
## to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
## copies of the Software, and to permit persons to whom the Software is
## furnished to do so, subject to the following conditions:
##
## The above copyright notice and this permission notice shall be included in
## all copies or substantial portions of the Software.
##
## THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
## IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
## FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
## AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
## LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
## OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
## THE SOFTWARE.
##
## -------------------------------------------------------------------
require 'erlbox'
require 'erlbox/driver'
require 'erlbox/snmp'
require 'net/http'

CLOBBER.include %w( c_src/system )

UNIT_TEST_FLAGS << '+A10'
INT_TEST_FLAGS << '+A10'

DB_TARBALL = 'c_src/db-4.7.25.tar.gz'
DB_LIB = "c_src/system/lib/libdb.a"

CC_FLAGS << "-Ic_src/system/include -pthread"
LD_LIBS  << DB_LIB
LD_FLAGS << "-pthread"

file DB_TARBALL do
  puts "Downloading BerkeleyDB..."
  # Not the fastest or most efficient method, but it works everywhere...
  # This does not rely on curl or wget being installed.
  Net::HTTP.start('download.oracle.com') do |http|
    resp = http.get('/berkeley-db/db-4.7.25.tar.gz')
    open(DB_TARBALL, "wb") {|file| file.write(resp.body) }
  end
end

file DB_LIB => DB_TARBALL do
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
