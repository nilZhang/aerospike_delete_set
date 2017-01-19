# import the module
from __future__ import print_function
import aerospike
import sys, getopt


opts,args = getopt.getopt(sys.argv[1:],"n:s:h:p:",["help"])
namespace=''
set=''
host='127.0.0.1'
port=3000

def usage():
  print("python aeroDeleteSet.py [-h host,default:127.0.0.1] [-p point:3000] -n namespace -s set")

for op, value in opts:
  if op == '-n':
     namespace = value
  elif op == '-s':
     set = value
  elif op == '-h':
     host = value
  elif op == '-p':
     port= value
  elif op == '--help':
     usage()
     sys.exit()
if namespace == '' or set == '':
  usage()
  sys.exit()

# Configure the client
config = {
  'hosts': [ ( host , port) ]
}

# Create a client and connect it to the cluster
try:
  print(config)
  client = aerospike.client(config).connect()
except:
  import sys
  print("failed to connect to the cluster with", config['hosts'])
  sys.exit(1)

# Records are addressable via a tuple of (namespace, set, key)


deleteCount=0;
def print_result((key, metadata, record)):
  global deleteCount
  deleteCount += 1
  print('delete:',deleteCount,record)
  try:
    deleteKey=(namespace,set,record['key'])
    client.remove(deleteKey)
  except Exception as e:
    import sys
    print("error: {0}".format(e), file=sys.stderr)
try:
  # Write a record
  scan = client.scan(namespace,set)
  scan.select('key')
  scan.foreach(print_result)

except Exception as e:
  import sys
  print("error: {0}".format(e), file=sys.stderr)

# Read a record
# Close the connection to the Aerospike cluster
client.close()
