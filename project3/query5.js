// Find the oldest friend for each user who has a friend. 
// For simplicity, use only year of birth to determine age. If there is a tie, use the friend with the smallest user_id
// Return a javascript object : the keys should be user_ids and the value for each user_id is their oldest friend's user_id
// You may find query 2 and query 3 helpful. You can create selections if you want. Do not modify the users collection.
//
//You should return something like this:(order does not matter)
//{user1:oldestFriend1, user2:oldestFriend2, user3:oldestFriend3,...}

function oldest_friend(dbname){
  db = db.getSiblingDB(dbname);
  var results = {};
  // TODO: implement oldest friends
  // return an javascript object described above
  return results
}
