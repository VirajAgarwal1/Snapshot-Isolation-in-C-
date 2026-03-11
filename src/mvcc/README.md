## Interface

This will create the MVCC. This will basically, present a wrapper on top of the naive storage engine. It will expose the following methods to the user:
1. Start Transaction: This method will give back a transaction id which should be used for further operations inside this transaction.
2. "Get" Operation: Given the transaction id, and the key whose value you want to get. This operation will give you its value (just 1).
3. "Set" Operation: Given the txn id, and the key whose value you want to set. This op will set its value.
4. "Delete" Operation: Given the txn id, and the key you want to delete. This op will delete its value, by updating the currently latest value (according to the snapshot of the transaction) and adding a new value with the same key as a tombstone.
5. "Commit" Transaction: This will mark all the operations in this txn as committed. And this txn cannot be used further, once closed.
6. "Abort" Transaction: This will abort the transaction. Nullying all of the changes made in the txn.
7. "Vaccum": This will clear up wates/unused space in the mvcc and helps in keeping the system healthy.

## Data Structures

This mvcc will be all in memory only, as my "naive storage engine" is also completely in memory. No point in having mvcc be durable with WAL, when the storage engine itself is not durable/persistent.

The mvcc will keep certain data structures with itself alive all the time:
1. Set of **non-committed/active** transactions.
    1. A mutex for this set, as we dont want this structure to be corrupted with concurrant updates. This will be a reader/writer lock pattern. The reader writer lock will use the C++ shared_mutex.
2. Next Transaction Id. This is just a basic counter which counts up from 1. This will help us transaction ids with global ordering.
    1. This should be an atomic variable since it will be just read and add 1 to.
3. A hashmap where the key is transaction id, and the value is a tuple with 3 things (these 3 together make the definition for our snapshot structure):
    1. A set of all the transactions which were active/uncommitted transactions at the time when the snapshot was made.
    2. The min transaction id of all the transactions in the array.
    <!-- 3. The next Transaction id as in when the snapshot was taken. -->
    This structure will also be protected by a shared_mutex.
4. A set of all txns which have committed. This will be protected by a shared_mutex as well.

## Extra

In the metadata of key key value pair I will store the following:
1. Transaction ID: Of the transaction which created/updated this version of the value
2. IsDead: Boolean telling if this version is tombstone or not. If this is true then it means that the transaction had meant to delete this key.


## Algorithm

What MVCC will internally do for each of the transactions:
1. Start Transaction
    1. Note the "next transaction id" 
    2. Acquire read lock on the active transactions set.
    3. Copy the active transaction set.
    4. Release read lock on the active transactions set.
    5. Compute the minimum transaction in this set.
    6. Acquire write lock on the hashmap of transactions.
    7. Add this transaction to the hashmap.
    8. Release write lock on the hashmap of txn.
    9. Return the txn id.
2. Get Operation
    1. Acquire read lock on the txn hashmap
    2. Get the snapshot info from the hashmap using the id.
    3. Release read lock on the txn hashmap
    4. Retrieve all the values for the key from the storage engine.
    5. Go through each value's version and get the versions which satisfy the following conditions (in conjunction):
        1. value's TransID (vtid) < SnapShot's min TransID (stid_min)
        2. vtid is NOT IN snapshot's set of active transactions at the time (s_act_t)
        3. vtid txn is IN the committed txns set
        
        **OR** it could just satisfy this condition:
        1. vtid == txn id under which this operation is working.
    6. Once we got the chain of versions visible to this snapshot, we just return value with the latest value, i.e. which has the highest transaction id. If the latest value was marked as a tombstone in its metadata, then we return that the key has been deleted. If our filtered chain has no versions then report back that the key doesnt exist.


## TODO:
- Maintain active transactions set properly. Since, if some txn aborts/completed but it didn't get reflected in the active set, then we have a problem. My whole MVCC Snapshot Isolation system relies on this assumption that the active is an accurate depiction of the current state of all transactions.