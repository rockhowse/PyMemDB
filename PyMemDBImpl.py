'''
    2016-03-23 ~ Nate Rock ~ Redis-like In Memory Database
    depenencies: Python 2.7.x
'''

# needed for sdin and stdout
import sys

# Class that implements a TransactionBlock, with a reference to its parent

class TransactionBlock:
    '''
        TransactionBlock ~ a simple class that stores a list of commands and a reference to a parent transaction block
    '''

    # need this for recursive commits
    parent_tb = None

    # list of commands in this transaction block
    cmd_list = []

    def __init__(self, parent=None):
        self.cmd_list = []
        self.parent_tb = parent

    def add_cmd(self, command_str):
        self.cmd_list.append(command_str)

    def set_parent(self, tb):
        self.parent_tb = tb

    def get_parent(self):
        return self.parent_tb

    def get_cmd_list(self):
        return self.cmd_list

    # recursively commit blocks until all commands have been committed
    # don't define this as an instance function and rely on self or we get a max rec depth exceeded
    @staticmethod
    def recursive_commit_blocks(smdb, tb):
        for cur_cmd in tb.get_cmd_list():
            # if it's a TransactionBlock, commit it!
            if cur_cmd.__class__.__name__ == "TransactionBlock":
                cur_cmd.recursive_commit_blocks(smdb, cur_cmd)

                # remove this commit block
                tb.get_cmd_list().remove(cur_cmd)

    # roll-back the current commit block
    def roll_back_current(self, smdb):

        # since we are rolling back, reverse the list
        self.cmd_list.reverse()

        # now run each command in reverse order to preserve integrity
        for cmd_to_reverse in self.cmd_list:

            # each t-log entry has the name and value of the old
            # need to handle non-existent or removed values
            for cmd_name in cmd_to_reverse:

                # get the old value so we can re-set it
                old_value = cmd_to_reverse[cmd_name]

                # if the previous value was "NULL" (assuming they didn't PUT it to null, explicitly =P)
                # then we can "DELETE" it, effectively removing it from the data store
                if old_value == "NULL":

                    # clear it out as it didn't previously exist
                    # set rollback-mode to True, needed after we add to log in the cmd_DELETE function
                    smdb.cmd_DELETE(cmd_name, True)

                # other wise, let's update it using whatever original value was there
                else:

                    # set the value using the previous value
                    # should handle multiple sets in the same transaction block of the same value
                    # set rollback-mode to True, needed after we add to log in the cmd_DELETE function
                    smdb.cmd_PUT(cmd_name, old_value, True)

class PyMemDB:
    '''
        PyMemDB ~ a simple name/value in-memory data store that supports nested t-log capabilities
    '''

    # lets you turn on debugging so you can trace commands
    enable_debugging = False

    # we can implement the in-memory database using a python dictionary as it satisfies the O(log n) requirement
    mem_db_dict = {}

    # implement separate value count cache to satisfy O(log n) requirement on count lookup
    mem_db_value_count_dict = {}

    # python's list data structure can pop() so we will use that for our queue data structure
    mem_db_transaction_blocks = None

    # reference to the current transaction block we are adding commands to
    mem_db_current_transaction_block = None

    # no transaction message
    no_transaction_msg = "NO TRANSACTION"

    def __init__(self, debugging=False):
        self.enable_debugging = debugging
        self.mem_db_dict = {}
        self.mem_db_transaction_blocks = None
        self.mem_db_current_transaction_block = None
        self.mem_db_value_count_dict = {}

    ''' =============  HELPER functions ==========
        custom functions needed to fully flush out all the requirements
    '''

    # get the size of the database, useful for asserts in unit tests
    def get_mem_db_size(self):
        return self.mem_db_dict.__len__()

    # see if we have a current commit block
    def is_in_commit_block(self):
        if self.mem_db_current_transaction_block:
            return True
        else:
            return False

    # get the number of commands blocks in the current transaction block
    def get_num_cmds_in_current_transaction_block(self):
        if self.is_in_commit_block():
            return self.mem_db_current_transaction_block.get_cmd_list().__len__()
        else:
            return 0

    ''' =============  PROCESS command functions ==========
        contains actual processing of the SIMPLE and TRANSACTION commands
    '''

    # process split simple commands
    def process_simple_command(self, split_cmd):
        cur_cmd = split_cmd[0].strip()

        # PUT command expects 2 parameters, simple length check here
        if cur_cmd == "PUT" and split_cmd.__len__() == 3:

            name = split_cmd[1].strip()
            value = split_cmd[2].strip()

            simple_mem_db.cmd_PUT(name, value)

            return True

        # PULL command expects 1 parameters, simple length check here
        elif cur_cmd == "PULL" and split_cmd.__len__() == 2:

            name = split_cmd[1].strip()

            print simple_mem_db.cmd_PULL(name)

            return True

        # DELETE command expects 1 parameters, simple length check here
        elif cur_cmd == "DELETE" and split_cmd.__len__() == 2:

            name = split_cmd[1].strip()

            # clear the entry, NO output
            simple_mem_db.cmd_DELETE(name)

            return True

        # NUM_WITH_VALUE command expects 1 parameters, simple length check here
        elif cur_cmd == "NUM_WITH_VALUE" and split_cmd.__len__() == 2:

            value = split_cmd[1].strip()

            # value is numeric convert to string
            print str(simple_mem_db.cmd_NUM_WITH_VALUE(value))

            return True

        # QUIT command expects 0 parameters, simple length check here
        elif cur_cmd == "QUIT" and split_cmd.__len__() == 1:

            # call the cmd_QUIT in case we want to do something in the class later
            simple_mem_db.cmd_QUIT()

            #stop processing
            return False

    # process split transaction commands
    def process_transaction_command(self, split_cmd):
            cur_cmd = split_cmd[0].strip()

            # START_COMMIT command expects 0 parameters, simple length check here
            if cur_cmd == "START_COMMIT" and split_cmd.__len__() == 1:
                simple_mem_db.cmd_START_COMMIT()

            # COMMIT command expects 0 parameters, simple length check here
            elif cur_cmd == "COMMIT" and split_cmd.__len__() == 1:
                if not simple_mem_db.cmd_END_COMMIT():
                    print self.no_transaction_msg

            # UN_COMMIT command expects 0 parameters, simple length check here
            elif cur_cmd == "UN_COMMIT" and split_cmd.__len__() == 1:
                if not simple_mem_db.cmd_UN_COMMIT():
                    print self.no_transaction_msg

    '''================== SIMPLE commands ==================
        PUT(name, value)
            -name  - the key we will use to store the value
            -value - the value which we want to set for the passed in key (name)

            *note* Neither variable names nor values will contain spaces
                   Best to check for this condition anyways.

        PULL(name)
            -name - print out the value previously assigned to the key (name)
                   if this key does not exist, print out "NULL"

        DELETE(name)
            -name - remove the entry associated with the key (name)

        NUM_WITH_VALUE(value)
            -value - print out the number of variables set to the passed in value
                     if no variables are equal to the passed in value, print "0"

        QUIT()
            - exit the program
    '''

    def cmd_PUT(self, name, value, roll_back_mode=False):

        # if we currently have open transaction blocks, start adding to t-log
        # after unit testing, we use the PUT value in rollback as well, so DON'T log while rolling back
        if self.mem_db_current_transaction_block is not None and not roll_back_mode:
            old_val = self.cmd_PULL(name)

            # t-log record contains {name: old_val}
            self.mem_db_current_transaction_block.add_cmd({name: old_val})

        # increment count cache for specific value
        if value in self.mem_db_value_count_dict:
            self.mem_db_value_count_dict[value] += 1
        else:
            self.mem_db_value_count_dict[value] = 1

        self.mem_db_dict[name] = value

    def cmd_PULL(self, name):

        if name in self.mem_db_dict:
            return self.mem_db_dict[name]
        else:
            return "NULL"

    def cmd_DELETE(self, name, roll_back_mode=False):

        # if we currently have open transaction blocks, start adding to t-log
        if self.mem_db_current_transaction_block is not None and not roll_back_mode:
            old_val = self.cmd_PULL(name)

            # t-log record contains {name: old_val}
            self.mem_db_current_transaction_block.add_cmd({name: old_val})

        cur_value = None

        if name in self.mem_db_dict:
            cur_value = self.cmd_PULL(name)

            del self.mem_db_dict[name]

        # increment count for specific value
        if cur_value is not None and cur_value in self.mem_db_value_count_dict:
             self.mem_db_value_count_dict[cur_value] -= 1

        # final check to remove the value count cache
        if self.mem_db_value_count_dict[cur_value] == 0:

            # remove this value from the count cache
            del self.mem_db_value_count_dict[cur_value]

    def cmd_NUM_WITH_VALUE(self, value):
        return self.mem_db_value_count_dict[value]

    def cmd_NUM_WITH_VALUE_SLOW(self, value):
        count_value = 0

        for cur_name in self.mem_db_dict:
            if self.mem_db_dict[cur_name] == value:
                count_value += 1

        return count_value

    def cmd_QUIT(self):
        return "TheCakeIsALie!"

    '''================== TRANSACTION commands ==================
        START_COMMIT()
            - Open a new transaction block. Transaction blocks can be nested; a START_COMMIT can be issued instead of an existing block
            - *note* this was the tricky part!

        UN_COMMIT()
            - Undo all of the commands issues in the MOST RECENT transaction block, and close the block.
            - Print nothing if successful
            - Print "NO TRANSACTION" if no transaction is in progress

        END_COMMIT()
            - Close all open transaction blocks, permanently applying the changes made in them.
            - Print nothing if successful
            - Print "NO TRANSACTION" if no transaction is in progress

    '''

    def cmd_START_COMMIT(self):

        # new transaction block
        new_transaction_block = TransactionBlock()

        # if we have a current block, assign it as parent for the new and swap new to the current
        if self.mem_db_current_transaction_block:
            new_transaction_block.set_parent(self.mem_db_current_transaction_block)
            self.mem_db_current_transaction_block = new_transaction_block

            new_transaction_block.get_parent().add_cmd(new_transaction_block)

        # no transaction blocks, so set the "head" as the current
        elif self.mem_db_transaction_blocks is None:
            self.mem_db_transaction_blocks = new_transaction_block
            self.mem_db_current_transaction_block = self.mem_db_transaction_blocks

    def cmd_UN_COMMIT(self):
        if self.mem_db_current_transaction_block:

            # roll back the current transaction block
            self.mem_db_current_transaction_block.roll_back_current(self)

            parent_block = self.mem_db_current_transaction_block.get_parent()

            if parent_block:
                parent_block.get_cmd_list().remove(self.mem_db_current_transaction_block)
                self.mem_db_current_transaction_block = parent_block

            return True
        else:
            # need to output NO TRANSACTION if no current TB
            return False

    def cmd_END_COMMIT(self):
        if self.mem_db_transaction_blocks:
            # let's process the transaction blocks recursively to keep them in order since START_COMMIT is nested
            self.mem_db_transaction_blocks.recursive_commit_blocks(self, self.mem_db_transaction_blocks)

            # clear out the current block
            self.mem_db_current_transaction_block = None

            # clear out the head block
            self.mem_db_transaction_blocks = None

            return True
        else:
            # need to output NO TRANSACTION if no TBs
            return False

# start up the application and listen to our PyMemDB commands!
if __name__ == "__main__":

    # enable/disable debugging
    allow_debug = False;

    # commands we will accept, all others will be ignored
    allowed_commit_commands = ["PUT", "DELETE"]
    allowed_simple_commands = allowed_commit_commands + ["PULL", "NUM_WITH_VALUE", "QUIT"]
    allowed_transaction_commands = ["START_COMMIT", "COMMIT", "UN_COMMIT"]

    # implementation of the simple memory db
    simple_mem_db = PyMemDB()

    # start reading in the first command
    mem_db_cmd = sys.stdin.readline()

    # keep reading commands until we receive "QUIT"
    while mem_db_cmd:
        split_cmd = mem_db_cmd.split(" ")

        # the first entry is always the command
        cur_cmd = split_cmd[0].strip()

        # if it's a simple command, handle it here
        if cur_cmd in allowed_simple_commands:
            if allow_debug:
                print "SIMPLE: " + mem_db_cmd

            # process simple commands until we get an QUIT which returns false
            if not simple_mem_db.process_simple_command(split_cmd):
                break

        # if it's a transaction command, handle it here
        elif cur_cmd in allowed_transaction_commands:
            if allow_debug:
                print "TRANSACTION" + mem_db_cmd

            simple_mem_db.process_transaction_command(split_cmd)

        # all other commands will be ignored

        # pull in the next command
        mem_db_cmd = sys.stdin.readline()