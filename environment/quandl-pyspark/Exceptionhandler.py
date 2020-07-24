#!/usr/bin/python

try:
   fh = open("Exception-handler", "w")
   cvsFile = open('/home/hpinto/Desktop/mySpark-data/DimCurrency.csv', "r")
   try:
      fh.write("This is my test file for exception handling!!")
   finally:
      print("Going to close the file")
      fh.close()
except IOError:
   fh.write("Error: can\'t find file or read data or Raised for operating system-related errors")
except EOFError:
   print("Error: Raised when there is no input from either the raw_input() or input() function and the end of file is reached.")
except LookupError:
   print("Error: class for all lookup errors.")


