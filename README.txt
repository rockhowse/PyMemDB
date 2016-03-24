This project contains two files:

    PyMemDBImpl.py ~ simple Redis-like in-memory DB that reads from stdio and writes to stdout and implements the following functions:

        ================== SIMPLE commands ==================
        PUT(name, value)
            -name  - the key we will use to store the value
            -value - the value which we want to set for the passed in key (name)

            *note* Neither variable names nor values will contain spaces
                   Best to check for this condition anyways.

        PULL(name)
            -name - print out the value previously assigned to the key (name)
                   if this key does not exist, print out "NULL"
-
        DELETE(name)
            -name - remove the entry associated with the key (name)

        NUM_WITH_VALUE(value)
            -value - print out the number of variables set to the passed in value
                     if no variables are equal to the passed in value, print "0"

        QUIT()
            - exit the program

        ================== TRANSACTION commands ===============
        START_COMMIT()
            - Open a new transaction block. 
            - *note* Transaction blocks can be nested; a START_COMMIT can be issued instead of an existing block

        UN_COMMIT()
            - Undo all of the commands issues in the MOST RECENT transaction block, and close the block.
            - Print nothing if successful
            - Print "NO TRANSACTION" if no transaction is in progress

        END_COMMIT()
            - Close all open transaction blocks, permanently applying the changes made in them.
            - Print nothing if successful
            - Print "NO TRANSACTION" if no transaction is in progress

    PyMemDBTests.py - unit tests for all the functions outlined above attempting to cover both common usage and edge-cases encountered during implementation

            ===== SIMPLE tests =====
            Test_cmd_PUT()
            Test_cmd_PULL()
            Test_cmd_DELETE()
            Test_cmd_NUM_WITH_VALUE()
            Test_cmd_QUIT()

            ===== TRANSACTION tests =====
            Test_cmd_START_COMMIT()
            Test_cmd_UN_COMMIT()
            Test_cmd_STOP_COMMIT()

            ===== PERFORMANCE tests =====
            TestBigODifferences()