#Sohith Chandra Naidu Tandra (1001715618)
#Madhulika Lekkala (1001718053)
import mysql.connector

# Connection with the my sql established
conn = mysql.connector.connect(user='root', password='', host='localhost', database='project1', auth_plugin='mysql_native_password')
cursor = conn.cursor()
# Provide the input name of the file as input.txt
print("Please enter filename : ")
fileName = input()
fileOutput = open("Output/Output.txt","w+")

# Method descibed for  mainting the wounnd wait transaction scheme
def woundWait_transaction(transactionOnHold, requestForTransaction, item, line):
    cursor.execute('select TtimeStamp from transaction where Tid = \'' + transactionOnHold + '\'')
    data = cursor.fetchall()
    h_time_stamp = 0
    for d in data:
        h_time_stamp = d[0]
    cursor.execute('select TtimeStamp from transaction where Tid = \'' + requestForTransaction + '\'')
    data = cursor.fetchall()

    r_time_stamp = 0
    for d in data:
        r_time_stamp = d[0]
    if r_time_stamp < h_time_stamp:
        abortHoldingTransaction(transactionOnHold)
    else:
        cursor.execute('Update transaction set Tstatus = \'Blocked\', operation = \'' + line + '\' where Tid = \'' + requestForTransaction + '\'')
        conn.commit()
        fileOutput.write('Transaction status '+requestForTransaction+' changed to wait \n')
        print('Transaction status '+requestForTransaction+' changed to wait')
        cursor.execute('select * from lockTable where item = \'' + item + '\'')
        data = cursor.fetchall()

        transactionsOnWait = ''
        for d in data:
            transactionsOnWait = d[3]
        transactionsOnWait += requestForTransaction + ';'
        cursor.execute('Update lockTable set Tid_waiting = \'' + transactionsOnWait + '\' where item = \'' + item + '\'')
        conn.commit()

# Method used for unlocking the transcations
def unlockRequestedTransaction(currTransac, item):
    cursor.execute('Select * from lockTable where item = \'' + item + '\'')
    data = cursor.fetchall()

    waitingTransactionList = []
    transactionOnHold = ''
    transactionOnHoldList = []
    transactionsToBeUpdated = ''

    for d in data:
        waitingTransactionList = d[3].split(';')
        for row in d[2].split(';'):
            if row != '' and row != currTransac:
                transactionOnHold += row + ';'
                transactionOnHoldList.append(row)

    if len(transactionOnHoldList) == 0:
        cursor.execute('Update locktable set state = \'Unlocked\', Tid_holding = \'' + transactionOnHold + '\' where item = \'' + item + '\'')
        conn.commit()
    else:
        cursor.execute('Update locktable set Tid_Holding = \'' + transactionOnHold + '\' where item = \'' + item + '\'')
        conn.commit()

    if '' in waitingTransactionList:
        waitingTransactionList.remove('')
    if len(waitingTransactionList) != 0:
        currentTransactionWaitList = waitingTransactionList[0]
        waitingTransactionList.remove(currentTransactionWaitList)

        for transaction in waitingTransactionList:
            transactionsToBeUpdated += transaction + ';'

        cursor.execute('Update locktable set Tid_waiting = \'' + transactionsToBeUpdated + '\' where item = \'' + item + '\'')
        fileOutput.write('Operations of the transaction '+ currentTransactionWaitList +' has been resumed \n')
        print('Operations of the transaction '+ currentTransactionWaitList +' has been resumed ')
        conn.commit()
        cursor.execute('Update transaction set Tstatus = \'Active\' where Tid = \'' + currentTransactionWaitList + '\'')
        conn.commit()
        cursor.execute('select *  from transaction where Tid = \'' + currentTransactionWaitList + '\'')
        data = cursor.fetchall()

        listOfOperationsOnWait = []
        for d in data:
            listOfOperationsOnWait = d[4].split(';')

        finalWaitingOperationList = []
        for operation in listOfOperationsOnWait:
            if operation != '':
                waitingTransactionList.append(operation)

        for operation in listOfOperationsOnWait:
            if operation != '':
                if operation[0] == 'b':
                    beginTransaction(operation[1], 1)

                elif operation[0] == 'r':
                    readLock(operation[3], operation[1], operation)

                elif operation[0] == 'w':
                    writeLock(operation[3], operation[1], operation)

                elif operation[0] == 'e':
                    commitRequestedTransaction(operation[1])

                operationsToBeUpdated = ''
                for ops in finalWaitingOperationList:
                    if ops != operation:
                        operationsToBeUpdated += ops
                cursor.execute('Update transaction set operation = \'' + operationsToBeUpdated + '\' where Tid = \'' +currTransac + '\'')
                conn.commit()

# Method used for putting the read lock on item by a transcation
def readLock(item, currTransac, line):
    cursor.execute('select * from lockTable where item = \'' + item + '\'')
    data = cursor.fetchall()
    itemsPresentInTransactions = ''
    transactionHoldingList = []

    for d in data:
        currentItem = d
        transactionHoldingList = d[2].split(';')

    if len(data) == 0:
        cursor.execute('Insert into lockTable values (\'' + item + '\', \'readlocked\', \'' + currTransac + ';\',\'\')')
        cursor.execute('select * from Transaction where Tid = \'' + currTransac + '\'')
        data = cursor.fetchall()

        for row in data:
            itemsPresentInTransactions += row[3] + ';'
        itemsPresentInTransactions += item + ';'

        cursor.execute('Update transaction set items = \'' + itemsPresentInTransactions + '\' where Tid = \'' + currTransac + '\'')
        fileOutput.write('Read lock on item '+item+' by transaction '+currTransac+'\n')
        print('Read lock on item '+item+' by transaction '+currTransac)
        conn.commit()
    else:
        if currentItem[1] == 'writelocked':
            woundWait_transaction(transactionHoldingList[0], currTransac, item, line)
        else:
            transactionsOnHold = ''
            for d in data:
                transactionsOnHold += d[2] + ';'
            transactionsOnHold += currTransac+';'
            cursor.execute('Update lockTable set Tid_Holding = \'' + transactionsOnHold + '\', state = \'readlocked\' where item = \'' + item + '\'')
            cursor.execute('select items from transaction where Tid = \'' + currTransac + '\'')
            data = cursor.fetchall()

            itmesToBeUpdated = ''
            for d in data:
                itmesToBeUpdated += d[0] + ';'
            itmesToBeUpdated += item + ';'

            cursor.execute('Update transaction set items = \'' + itmesToBeUpdated + '\' where Tid = \'' + currTransac + '\'')
            fileOutput.write('Transaction ' + currTransac + ' obtained read lock on item ' + item+'\n')
            print('Transaction ' + currTransac + ' obtained read lock on item ' + item)
            conn.commit()

# Method used for begining any transaction
def beginTransaction(currTransac, time_stamp):
    cursor.execute('insert into transaction values (\'' + currTransac + '\', ' + str(time_stamp) + ', \'Active\', \'\',\'\')')
    fileOutput.write('Begin Transaction :' + currTransac+'\n')
    print('Begin Transaction :' + currTransac)
    conn.commit()

# Method used to commit transactions that are completed
def commitRequestedTransaction(currTransac):
    cursor.execute('select * from transaction where Tid = \'' + currTransac + '\'')
    data = cursor.fetchall()
    fileOutput.write('Transaction ' + currTransac + ' committed \n')
    print('Transaction ' + currTransac + ' committed')
    cursor.execute('Update transaction set Tstatus = \'commited\', items = \'\' where Tid = \'' + currTransac + '\'')
    conn.commit()
    for d in data:
        for items in d[3].split(';'):
            unlockRequestedTransaction(currTransac,items)

# Method used for reading a input file provided
def readFile():

    cursor = conn.cursor()
    cursor.execute('truncate transaction')
    cursor.execute('truncate lockTable')
    conn.commit()
    f = open(fileName, 'r')
    time_stamp = 0
    for line in f.readlines():
        line = line.replace(' ', '')
        cursor.execute('Select * from transaction where Tid = \'' + line[1] + '\'')
        data = cursor.fetchall()
        transactionList = []
        for item in data:
            transactionList = item

        if len(transactionList) == 0:
            if line[0] == 'b':
                time_stamp += 1
                beginTransaction(line[1], time_stamp)
            elif line[0] == 'r':
                readLock(line[3], line[1], line)
            elif line[0] == 'w':
                writeLock(line[3], line[1], line)
            elif line[0] == 'e':
                commitRequestedTransaction(line[1])
        else:
            if transactionList[2] == 'Blocked':
                cursor.execute('update transaction set operation = \'' + transactionList[4]+line + '\' where Tid = \'' + line[1] + '\'')
                fileOutput.write('Operation '+line+ ' added to waiting list of operations which belongs to Transaction '+ line[1]+'\n')
                print('Operation ' + line + ' Added to waiting operation list for Transaction ' + line[1])
                conn.commit()
            elif transactionList[2] == 'Aborted':
                fileOutput.write('Transaction '+line[1]+' is aborted \n')
                print('Transaction '+line[1]+' is aborted ')
            else:
                if line[0] == 'b':
                    beginTransaction(line[1], time_stamp)
                elif line[0] == 'r':
                    readLock(line[3], line[1], line)
                elif line[0] == 'w':
                    writeLock(line[3], line[1], line)
                elif line[0] == 'e':
                    commitRequestedTransaction(line[1])

# Method used for providing a write lock on the file
def writeLock(item, currentTransaction, line):

    cursor.execute('select * from lockTable where item = \'' + item + '\'')
    data = cursor.fetchall()
    transactionHoldingList = []
    transactionWaitingList = []

    currentItem = ''
    for d in data:
        currentItem = d
        transactionHoldingList = d[2].split(';')
        transactionWaitingList = d[3].split(';')

    if (currentItem !='' and currentItem[1] == 'readlocked') and (currentItem[2].replace(';','') == currentTransaction):
        cursor.execute('Update lockTable set state = \'writelocked\' where item = \'' + item + '\'')
        fileOutput.write('Upgrade lock from read to write on item '+item+' by transaction '+currentTransaction+'\n')
        print('Upgrade lock from read to write on item '+item+' by transaction '+currentTransaction)
        conn.commit()

    elif currentItem !='' and currentItem[1] == 'Unlocked':
        transactionsOnHold = ''
        for d in data:
            transactionsOnHold += d[2] + ';'
        transactionsOnHold += currentTransaction + ';'

        cursor.execute('Update lockTable set state = \'writelocked\', Tid_Holding = \''+ transactionsOnHold + '\' where item = \'' + item + '\'')
        conn.commit()
        cursor.execute('select items from transaction where Tid = \'' + currentTransaction + '\'')
        data = cursor.fetchall()

        itemsTobeUpdated = ''
        for d in data:
            itemsTobeUpdated += d[0] + ';'
        itemsTobeUpdated += item + ';'

        cursor.execute('Update transaction set items = \'' + itemsTobeUpdated + '\' where Tid = \'' + currentTransaction + '\'')
        fileOutput.write('Write lock on item '+item+' by transaction '+currentTransaction+'\n')
        print('Write lock on item '+item+' by transaction '+currentTransaction)
        conn.commit()
    else:
        filteredListOfTransactionsOnHold = []
        for transaction in transactionHoldingList:
            if transaction != '' and transaction != currentTransaction:
                filteredListOfTransactionsOnHold.append(transaction)
        length_holdTrsanctions = len(filteredListOfTransactionsOnHold)

        if length_holdTrsanctions == 1:
            woundWait_transaction(filteredListOfTransactionsOnHold[0], currentTransaction, item, line)
        else:
            cursor.execute('select * from transaction where Tid = \'' + currentTransaction + '\'')
            data = cursor.fetchall()
            time_stamp = 0
            decicionVariable = False
            for d in data:
                time_stamp = d[1]
            if length_holdTrsanctions > 0:
                for tid in filteredListOfTransactionsOnHold:
                    cursor.execute('select * from transaction where Tid = \'' + tid + '\'')
                    data = cursor.fetchall()
                    h_time_stamp = 0
                    for d in data:
                        h_time_stamp = d[1]
                    if time_stamp < h_time_stamp:
                        decicionVariable = True
                    else:
                        decicionVariable = False
                        break
                if decicionVariable == True:
                    cursor.execute('Update transaction set status = \'Blocked\', operation = \'' + line + '\' where Tid = \'' +currentTransaction + '\'')
                    conn.commit()

                    transactionOnWait = ''
                    for transaction in transactionWaitingList:
                        if transaction != '':
                            transactionOnWait += transaction + ';'
                    cursor.execute('update lockTable set Tid_waiting = \'' + transactionOnWait + '\' where item = \'' + item + '\'')
                    conn.commit()

                elif decicionVariable == False:
                    abortHoldingTransaction(currentTransaction)

# Method for aborting a transaction
def abortHoldingTransaction(currTransac):
    cursor.execute('select * from transaction where Tid = \'' + currTransac + '\'')
    data = cursor.fetchall()
    cursor.execute('Update transaction set Tstatus = \'Aborted\', items = \'\' where Tid = \'' + currTransac + '\'')
    fileOutput.write('Transaction ' + currTransac + '   aborted \n')
    print('Transaction ' + currTransac + '   aborted')
    conn.commit()
    for d in data:
        for items in d[3].split(';'):
            unlockRequestedTransaction(currTransac,items)
if __name__ == '__main__':
    readFile()
