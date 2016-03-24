'''
    2016-03-23 ~ Nate Rock ~ Redis-like In Memory Database
    quick test script for testing the SimpleMemDB
    depenencies: Python 2.7.x

    2016-03-21 ~ added HUGE testing for O(n) vs O(log n) timing
'''
import timeit

from PyMemDBImpl import PyMemDB

''' ========= SIMPLE command tests ======== '''

# testing the PUT command
def Test_cmd_PUT():
    simple_test_db = PyMemDB()
    simple_test_db.cmd_PUT("herp", "derp")

    # there should be one name/value pair in the database
    assert (simple_test_db.get_mem_db_size() == 1)

    print "cmd_PUT: passed"

# testing the PULL command
def Test_cmd_PULL():
    simple_test_db = PyMemDB()
    simple_test_db.cmd_PUT("herp", "derp")

    # there should be one name/value pair in the database
    assert (simple_test_db.get_mem_db_size() == 1)

    # herp should be set to derp
    assert (simple_test_db.cmd_PULL("herp") == "derp")

    # getting any other name should result in NULL
    assert (simple_test_db.cmd_PULL("") == "NULL")
    assert (simple_test_db.cmd_PULL("not_exist") == "NULL")

    print "cmd_PULL: passed"

# testing the DELETE command
def Test_cmd_DELETE():
    simple_test_db = PyMemDB()
    simple_test_db.cmd_PUT("herp", "derp")

    # there should be one name/value pair in the database
    assert (simple_test_db.get_mem_db_size() == 1)

    # herp should be set to derp
    assert (simple_test_db.cmd_PULL("herp") == "derp")

    # clear out the key "herp"
    simple_test_db.cmd_DELETE("herp")

    # herp should be removed and should return "NULL"
    assert (simple_test_db.cmd_PULL("herp") == "NULL")

    # there should be NO name/value values in the database
    assert (simple_test_db.get_mem_db_size() == 0)

    print "cmd_DELETE: passed"

# testing the NUM_WITH_VALUE command
def Test_cmd_NUM_WITH_VALUE():
    simple_test_db = PyMemDB()
    simple_test_db.cmd_PUT("herp", "derp")

    # there should be one name/value pair in the database
    assert (simple_test_db.get_mem_db_size() == 1)

    # there should be one name/value with value "derp"
    assert (simple_test_db.cmd_NUM_WITH_VALUE("derp") == 1)

    # add another name/value with value of "derp"
    simple_test_db.cmd_PUT("flerp", "derp")

    # there should be two name/value pairs in the database
    assert (simple_test_db.get_mem_db_size() == 2)

    # there should be two name/value pairs in the database with value to "derp"
    assert (simple_test_db.cmd_NUM_WITH_VALUE("derp") == 2)

    # add another name/value with a value of NOT "derp"
    simple_test_db.cmd_PUT("cake", "lie")

    # there should be three name/value pairs in the database
    assert (simple_test_db.get_mem_db_size() == 3)

    # there should be two name/value pairs in the database with value to "derp"
    assert (simple_test_db.cmd_NUM_WITH_VALUE("derp") == 2)

    print "cmd_NUM_WITH_VALUE: passed"

def Test_cmd_QUIT():
    simple_test_db = PyMemDB()

    assert(simple_test_db.cmd_QUIT() == "TheCakeIsALie!")

    print "cmd_QUIT: passed"

''' ======== TRANSACTION command tests ======== '''

# testing the START_COMMIT command
def Test_cmd_START_COMMIT():
    simple_test_db = PyMemDB()
    simple_test_db.cmd_PUT("herp", "derp")

    # there should be one name/value pair in the database
    assert (simple_test_db.get_mem_db_size() == 1)

    simple_test_db.cmd_START_COMMIT()

    # the PyMemDB should be in side a commit block
    assert(simple_test_db.is_in_commit_block() == True)

    # the current commit block should have no t-log commands
    assert(simple_test_db.get_num_cmds_in_current_transaction_block() == 0)

    # update the herp to "flerp" instead so we have something in the t-log
    simple_test_db.cmd_PUT("herp", "flerp")

    # the current commit block should have no t-log commands
    assert(simple_test_db.get_num_cmds_in_current_transaction_block() == 1)

    print "cmd_START_COMMIT: passed"

# testing the UN_COMMIT command
def Test_cmd_UN_COMMIT():
    simple_test_db = PyMemDB()
    simple_test_db.cmd_PUT("herp", "derp")

    # with no START_COMMIT, commit should respond with NO TRANSACTION
    assert(simple_test_db.cmd_UN_COMMIT() == False)

    # there should be one name/value pair in the database
    assert (simple_test_db.get_mem_db_size() == 1)

    simple_test_db.cmd_START_COMMIT()

    # the PyMemDB should be in side a commit block
    assert(simple_test_db.is_in_commit_block() == True)

    # the current commit block should have no t-log commands
    assert(simple_test_db.get_num_cmds_in_current_transaction_block() == 0)

    # update the herp to "flerp" instead so we have something in the t-log
    simple_test_db.cmd_PUT("herp", "flerp")

    # the current commit block should have no t-log commands
    assert(simple_test_db.get_num_cmds_in_current_transaction_block() == 1)

    # set a nested start_commit here
    simple_test_db.cmd_START_COMMIT()

    # the current commit block should have no t-log commands as this is a nested Transaction Block
    assert(simple_test_db.get_num_cmds_in_current_transaction_block() == 0)

    # update the herp to "flerp" instead so we have something in the t-log
    simple_test_db.cmd_PUT("herp", "derp2")

    # add new name/value in this transaction to test non-existing t-log rollback
    simple_test_db.cmd_PUT("onefish", "twofish")

    # the current commit block should have two t-log commands
    assert(simple_test_db.get_num_cmds_in_current_transaction_block() == 2)

    # the current value of herp should be derp2
    assert(simple_test_db.cmd_PULL("herp") == "derp2")

    # simple_test_db.cmd_ROLEBACK, should return True so we know it's in a Transaction Block
    assert(simple_test_db.cmd_UN_COMMIT() == True)

    # the current value of herp should be flerp
    assert(simple_test_db.cmd_PULL("herp") == "flerp")

    # the current value of onefish should have been rolled back
    assert(simple_test_db.cmd_PULL("onefish") == "NULL")

    # simple_test_db.cmd_ROLEBACK, should return True so we know it's in a Transaction Block
    assert(simple_test_db.cmd_UN_COMMIT() == True)

    # the current value of herp should be the original value of derp
    assert(simple_test_db.cmd_PULL("herp") == "derp")

    print "cmd_UN_COMMIT: passed"
    

# testing the END_COMMIT command
def Test_cmd_END_COMMIT():
    simple_test_db = PyMemDB()
    simple_test_db.cmd_PUT("herp", "derp")

    # with no START_END_COMMIT, commit should respond with NO TRANSACTION
    assert(simple_test_db.cmd_END_COMMIT() == False)

    # there should be one name/value pair in the database
    assert (simple_test_db.get_mem_db_size() == 1)

    simple_test_db.cmd_START_COMMIT()

    # the PyMemDB should be in side a commit block
    assert(simple_test_db.is_in_commit_block() == True)

    # the current commit block should have no t-log commands
    assert(simple_test_db.get_num_cmds_in_current_transaction_block() == 0)

    # update the herp to "flerp" instead so we have something in the t-log
    simple_test_db.cmd_PUT("herp", "flerp")

    # the current commit block should have one t-log commands
    assert(simple_test_db.get_num_cmds_in_current_transaction_block() == 1)

    # set a nested start_commit here
    simple_test_db.cmd_START_COMMIT()

    # the current commit block should have no t-log commands as this is a nested Transaction Block
    assert(simple_test_db.get_num_cmds_in_current_transaction_block() == 0)

    # update the herp to "flerp" instead so we have something in the t-log
    simple_test_db.cmd_PUT("herp", "derp2")

    # the current commit block should have no t-log commands
    assert(simple_test_db.get_num_cmds_in_current_transaction_block() == 1)

    # simple_test_db.cmd_END_COMMIT, should return True so we know it's in a Transaction Block
    assert(simple_test_db.cmd_END_COMMIT() == True)

    # the PyMemDB should no longer have any pending Transaction Blocks
    assert(simple_test_db.is_in_commit_block() == False)

    print "cmd_END_COMMIT: passed"
    

# O(log n) performing NUM_WITH_VALUE using value count cache
# slightly slower inserts/deletes, MUCH faster returns
def Test_cmd_NUM_WITH_VALUE_HUGE_O_N(simple_db):
    simple_db.cmd_NUM_WITH_VALUE_SLOW("a")

# O(n) performing NUM_WITH_VALUE using a simple iteration and value check (ick!!!!)
def Test_cmd_NUM_WITH_VALUE_HUGE_O_LOG_N(simple_db):
    simple_db.cmd_NUM_WITH_VALUE("a")

# not my idea, taken from
# http://pythoncentral.io/time-a-python-function/
def wrapper(func, *args, **kwargs):
    def wrapped():
        return func(*args, **kwargs)

    return wrapped

# test O(log n) vs O(n) value count changes
def TestBigODifferences():
    simple_test_db = PyMemDB()
    max_num_values = 10000000

    print "Populating PyMemDB with: " + str(max_num_values) + " key-value pairs with the same value. be patient"
    for i in range(0, max_num_values):
        simple_test_db.cmd_PUT(str(i), "a")

    wrapped = wrapper(Test_cmd_NUM_WITH_VALUE_HUGE_O_N, simple_test_db)
    time_o_n = timeit.timeit(wrapped, number=1)

    wrapped = wrapper(Test_cmd_NUM_WITH_VALUE_HUGE_O_LOG_N, simple_test_db)
    time_o_log_n = timeit.timeit(wrapped, number=1)

    print "O(log n) time: " + str(time_o_log_n) + " for " + str(max_num_values) + " records is " + str(time_o_n/time_o_log_n) + " times faster than O(n) time of " + str(time_o_n)

    # if we can return a count of 1,000,000 of the same value in under a second, this passes!
    assert(time_o_log_n < 1.0)

    print "O(log n) function: passed"

    # we assume the non O(n) will take longer than 1 second and fail
    assert(time_o_n > 1.0)

    print "O(n) function: failed"

# start up the application and listen to our SimpleMemDB commands!
if __name__ == "__main__":

    ''' ===== SIMPLE tests ===== '''
    Test_cmd_PUT()
    Test_cmd_PULL()
    Test_cmd_DELETE()
    Test_cmd_NUM_WITH_VALUE()
    Test_cmd_QUIT()

    ''' ===== TRANSACTION tests ===== '''
    Test_cmd_START_COMMIT()
    Test_cmd_UN_COMMIT()
    Test_cmd_END_COMMIT()

    ''' ===== PERFORMANCE tests ===== '''
    TestBigODifferences()
