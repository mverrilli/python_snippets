# python_snippets
Just my repository of strange and (maybe) interesting python code.

## tcp_middle_man.py 
This script is used to intercept TCP communications between a client and a server for the purpose of debugging the frames sent between the two.  It may not work in all cases as-is, so you may need to modify it for your own use.  You'll need to edit the script to change the ports and addresses.  
I used this to inspect AMQP frames in order to debug some issues I was having with a client library.  If you want a hex dump only, you'll need to change from basic_amqp_parser to hex_dump_noparser.

### basic_amqp_parser.py
Meant to be used with tcp_middle_man.py.  You send it streams of data as they are received from the sockets, and it will parse it as best it can and print the output.  I am sure there are some cases I haven't thought of that might cause it to continually look for more data and not ever print anything. YMMV.  This is implemented as a coroutine.  

### hex_dump_noparser.py
Meant to be used with tcp_middle_man.py.  You send it streams of data as they are received from the sockets, and it will simply print the hex strings that it sees.  Good for when you just want manually parse your frames as one-offs.  If you are going to do this often, write a parser for your protocol. This is implemented as a coroutine.  

## sql_functions.py
Python implementation of some common sql functions found in Vertica (and maybe Postgresql). Useful if you are converting formulas out of the database and into Python for transformation.
