#include "Join.hpp"
//#include <map>
//#include <set>
#include <functional>

/*
 * TODO: Student implementation
 * Input: Disk, Memory, Disk page ids for left relation, Disk page ids for right relation
 * Output: Vector of Buckets of size (MEM_SIZE_IN_PAGE - 1) after partition
 */
vector<Bucket> partition(
    Disk* disk, 
    Mem* mem, 
    pair<unsigned int, unsigned int> left_rel, 
    pair<unsigned int, unsigned int> right_rel) {
        // Handle left relation
        vector<Bucket> res;
        Page* inputBuffer;
        Page* temp;
        unsigned int hashIndex;
        // map<unsigned int, set<unsigned int>> leftRelPages;
        // map<unsigned int, set<unsigned int>> rightRelPages;
        for (unsigned int i = 0; i < MEM_SIZE_IN_PAGE - 1; ++i) {
            res.push_back(Bucket(disk));
        }
        for (unsigned int i = left_rel.first; i < left_rel.second; ++i) {
            mem->loadFromDisk(disk, i, MEM_SIZE_IN_PAGE - 1);
            inputBuffer = mem->mem_page(MEM_SIZE_IN_PAGE - 1);
            for (unsigned int j = 0; j < inputBuffer->size(); ++j) {
                Record tempRecord = inputBuffer->get_record(j);
                hashIndex = tempRecord.partition_hash() % (MEM_SIZE_IN_PAGE - 1);
                temp = mem->mem_page(hashIndex);
                temp->loadRecord(tempRecord);
                //leftRelPages[hashIndex].insert(i);
                if (temp->full()) {
                    res[hashIndex].add_left_rel_page(mem->flushToDisk(disk, hashIndex));
                    //leftRelPages[hashIndex].insert(mem->flushToDisk(disk, hashIndex));
                }
            }
        }
        for (unsigned int i = 0; i < MEM_SIZE_IN_PAGE - 1; ++i) {
            if (mem->mem_page(i)->size() != 0) {
                res[i].add_left_rel_page(mem->flushToDisk(disk, i));
                //leftRelPages[i].insert(mem->flushToDisk(disk, i));
            }
        }

        // Handle right relation
        for (unsigned int i = right_rel.first; i < right_rel.second; ++i) {
            mem->loadFromDisk(disk, i, MEM_SIZE_IN_PAGE - 1);
            inputBuffer = mem->mem_page(MEM_SIZE_IN_PAGE - 1);
            for (unsigned int j = 0; j < inputBuffer->size(); ++j) {
                Record tempRecord = inputBuffer->get_record(j);
                hashIndex = tempRecord.partition_hash() % (MEM_SIZE_IN_PAGE - 1);
                temp = mem->mem_page(hashIndex);
                temp->loadRecord(tempRecord);
                //rightRelPages[hashIndex].insert(i);
                if (temp->full()) {
                    res[hashIndex].add_right_rel_page(mem->flushToDisk(disk, hashIndex));
                   //rightRelPages[hashIndex].insert(mem->flushToDisk(disk, hashIndex));
                }
            }
        }
        for (unsigned int i = 0; i < MEM_SIZE_IN_PAGE - 1; ++i) {
            if (mem->mem_page(i)->size() != 0) {
                res[i].add_right_rel_page(mem->flushToDisk(disk, i));
                //rightRelPages[i].insert(mem->flushToDisk(disk, i));
            }
        }

        // Set up vector of buckets
        // map<unsigned int, set<unsigned int>>::iterator it;
        // for (it = leftRelPages.begin(); it != leftRelPages.end(); ++it) {
        //     set<unsigned int>::iterator it2;
        //     for (it2 = it->second.begin(); it2 != it->second.end(); ++it) {
        //         res[it->first].add_left_rel_page(*it2);
        //     }
        // }
        // for (it = rightRelPages.begin(); it != rightRelPages.end(); ++it) {
        //     set<unsigned int>::iterator it2;
        //     for (it2 = it->second.begin(); it2 != it->second.end(); ++it) {
        //         res[it->first].add_right_rel_page(*it2);
        //     }
        // }
        return res;
    }

/*
 * TODO: Student implementation
 * Input: Disk, Memory, Vector of Buckets after partition
 * Output: Vector of disk page ids for join result
 */
vector<unsigned int> probe(Disk* disk, Mem* mem, vector<Bucket>& partitions) {
    Page* inputBuffer;
    Page* temp;
    Page* temp2;
    unsigned int hashIndex;
    vector<unsigned int> res;
    for (unsigned int i = 0; i < MEM_SIZE_IN_PAGE - 1; ++i) {
        // check which relation is smaller
        vector<unsigned int> left_rel = partitions[i].get_left_rel();
        vector<unsigned int> right_rel = partitions[i].get_right_rel();
        if (partitions[i].num_left_rel_record <= partitions[i].num_right_rel_record) {
            for (unsigned int j = 0; j < left_rel.size(); ++j) {
                mem->loadFromDisk(disk, left_rel[j], MEM_SIZE_IN_PAGE - 1);
                inputBuffer = mem->mem_page(MEM_SIZE_IN_PAGE - 1);
                for (unsigned int k = 0; k < inputBuffer->size(); ++k) {
                    Record tempRecord = inputBuffer->get_record(k);
                    hashIndex = tempRecord.probe_hash() % (MEM_SIZE_IN_PAGE - 2);
                    temp = mem->mem_page(hashIndex);
                    temp->loadRecord(tempRecord);
                }
            }
            for (unsigned int j = 0; j < right_rel.size(); ++j) {
                mem->loadFromDisk(disk, right_rel[j], MEM_SIZE_IN_PAGE - 1);
                inputBuffer = mem->mem_page(MEM_SIZE_IN_PAGE - 1);
                for (unsigned int k = 0; k < inputBuffer->size(); ++k) {
                    Record tempRecord = inputBuffer->get_record(k);
                    hashIndex = tempRecord.probe_hash() % (MEM_SIZE_IN_PAGE - 2);
                    temp = mem->mem_page(hashIndex);
                    for (unsigned int l = 0; l < temp->size(); ++l) {
                        Record tempRecord2 = temp->get_record(l);
                        if (tempRecord == tempRecord2) {
                            temp2 = mem->mem_page(MEM_SIZE_IN_PAGE - 2);
                            temp2->loadPair(tempRecord, tempRecord2);
                            if (temp2->full()) {
                                res.push_back(mem->flushToDisk(disk, MEM_SIZE_IN_PAGE - 2));
                            }
                        }
                    }
                }
            }
        }
        else {
            for (unsigned int j = 0; j < right_rel.size(); ++j) {
                mem->loadFromDisk(disk, right_rel[j], MEM_SIZE_IN_PAGE - 1);
                inputBuffer = mem->mem_page(MEM_SIZE_IN_PAGE - 1);
                for (unsigned int k = 0; k < inputBuffer->size(); ++k) {
                    Record tempRecord = inputBuffer->get_record(k);
                    hashIndex = tempRecord.probe_hash() % (MEM_SIZE_IN_PAGE - 2);
                    temp = mem->mem_page(hashIndex);
                    temp->loadRecord(tempRecord);
                }
            }
            for (unsigned int j = 0; j < left_rel.size(); ++j) {
                mem->loadFromDisk(disk, left_rel[j], MEM_SIZE_IN_PAGE - 1);
                inputBuffer = mem->mem_page(MEM_SIZE_IN_PAGE - 1);
                for (unsigned int k = 0; k < inputBuffer->size(); ++k) {
                    Record tempRecord = inputBuffer->get_record(k);
                    hashIndex = tempRecord.probe_hash() % (MEM_SIZE_IN_PAGE - 2);
                    temp = mem->mem_page(hashIndex);
                    for (unsigned int l = 0; l < temp->size(); ++l) {
                        Record tempRecord2 = temp->get_record(l);
                        if (tempRecord == tempRecord2) {
                            temp2 = mem->mem_page(MEM_SIZE_IN_PAGE - 2);
                            temp2->loadPair(tempRecord, tempRecord2);
                            if (temp2->full()) {
                                res.push_back(mem->flushToDisk(disk, MEM_SIZE_IN_PAGE - 2));
                            }
                        }
                    }
                }
            }
        }
        for (unsigned int j = 0; j < MEM_SIZE_IN_PAGE - 2; ++j) {
            // reset the first 14 pages in memory
            mem->mem_page(j)->reset();
        }
    }
    //flush the 2nd to last page, if needed
    temp = mem->mem_page(MEM_SIZE_IN_PAGE - 2);
    if (temp->size() != 0) {
        res.push_back(mem->flushToDisk(disk, MEM_SIZE_IN_PAGE - 2));
    }
    return res;
}

