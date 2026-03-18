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
2. Next Transaction Id. This is just a basic counter which counts up from 1. This will help us transaction ids with global ordering. This counter initial value will be 1 and not 0.
    1. This should be an atomic variable since it will be just read and add 1 to.
3. A hashmap where the key is transaction id, and the value is a tuple with 3 things (these 3 together make the definition for our snapshot structure):
    1. A set of all the transactions which were active/uncommitted transactions at the time when the snapshot was made.
    2. The min transaction id of all the transactions in the array.
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
    2. Acquire write lock on the active transactions set.
    3. Copy the active transaction set.
    4. Add this transaction to the set.
    5. Release write lock on the active transactions set.
    6. Compute the minimum transaction in this set.
    7. Acquire write lock on the hashmap of transactions.
    8. Add this transaction to the hashmap.
    9. Release write lock on the hashmap of txn.
    10. Return the txn id.
2. Get Operation
    1. Acquire read lock on the txn hashmap
    2. Get the snapshot info from the hashmap using the id.
    3. Release read lock on the txn hashmap
    4. Acquire shared lock (reader lock) on the key.
    5. Retrieve all the values for the key from the storage engine.
    6. Release shared lock on the key.
    7. Go through each value's version and get the versions which satisfy the following conditions (in conjunction):
        1. value's TransID (vtid) < SnapShot's min TransID (stid_min)
        2. vtid is NOT IN snapshot's set of active transactions at the time (s_act_t)
        3. vtid txn is IN the committed txns set
        
        **OR** it could just satisfy this condition:
        1. vtid == txn id under which this operation is working.

        **OR** it could be a vaccummed version:
        1. vtid == 0
    8. Once we got the chain of versions visible to this snapshot, we just return value with the latest value, i.e. which has the highest transaction id. If the latest value was marked as a tombstone in its metadata, then we return that the key has been deleted. If our filtered chain has no versions then report back that the key doesnt exist.
3. Set Operation
    1. Acquire read lock on the txn hashmap
    2. Get the snapshot info from the hashmap using the id.
    3. Release read lock on the txn hashmap
    4. Acquire Exclusive lock (writer lock) on the key.
    5. Retrieve all the values for the key from the storage engine.
    6. Fetch the last version (one at the back of vector of values). If the version of this value satisfies these conditions then we will have to abort the txn because we have writer-writer conflict on our hands.
        1. vtid != txn id under which this operation is working.
        2. vtid >= stid_min
    6. Make the edit in the key in the storage engine.
    7. Release Exclusive lock on the key.
4. Delete Operation
    - Same as set operation. Instead of value you mark the verion as dead in the metadata.
5. Commit Transaction
    1. Acquire write lock on active txns set
    2. Remove this txn from the set
    3. Release write lock on active txns set
    4. Acquire write lock on the committed txns set.
    5. Add this txn to the set.
    6. Release write lock on the committed txns set.
    7. Acquire write lock on txn to snapshot hashmap
    8. Remove this txn from the hashmap.
    9. Release write lock on txn to snapshot hashmap
6. Abort Transaction
    1. Acquire write lock on active txns set
    2. Remove this txn from the set
    3. Release write lock on active txns set
7. Vaccum
    1. To clear txns from the txn-snapshot hashmap, we see all active txns. Whatever txns are not there in it but are there in this hashmap, will be removed.
    2. To clear the set of committed txns set, I will get the snapshot info for all the active txns right now. Then find the minimum of all the stid_min of these snapshots (lets call this new minimum as db_horizon). What all txns in the committed set are less than this horizon, we will remove them.
    3. To make space in the storage engine, I'll go to each key and iterate through each version until I find the one which has vtid greater than or equal to the db_horizon. The version just before this one (lets call it before_horizon_version) will be kept, and everything before it will be purged. We will also change the vtid of this before_horizon_version to 0. This will allow set and get operations to read it and automatically infer that this is committed txn without checking any other data structure.



## TODO:
- Maintain active transactions set properly. Since, if some txn aborts/completed but it didn't get reflected in the active set, then we have a problem. My whole MVCC Snapshot Isolation system relies on this assumption that the active is an accurate depiction of the current state of all transactions.
- Figure out a way to reset the global transaction id counter. The system will crash as soon as that counter reaches its maximum.
- In set and get operations add the condition that if the txn id is 0 then it is definitely visible.