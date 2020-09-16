# Rigorous-2PL-Concurrency-Control-Protocol:

Rigorous 2 PL concurrency control protocol with wound-wait method:

In Rigorous 2 PL all items that are writelocked or readlocked by a transaction are not unlocked until after the transaction commits. It guarantees strict schedules. In wound-wait, transactions only wait is on older transactions that started earlier i.e., if transaction requesting lock is older than that holding the lock, transaction holding the lock is preemptively aborted.

# Algorithm:

If Ti requests an item X that is held by Tj in conflicting mode, then
if TS(Ti) < TS(Tj) then Tj is aborted (Ti wounds younger Tj)
else Ti waits (on an older transaction Tj)

# Steps to be followed to run the code:

1.Create the database with given script file and change other details in code accordingly. 
2.Run the tables.sql file to generate table in your database.
3.Place input.txt in the project. 
4.You need to install the mysql.connector package for python.
      You can run the code using the following command:
      python twophaselocking.py 
      Pass the input argument as input.txt
5.A output.txt will be generated according to the input given in the file input.txt.
