// query3
// create a collection named "cities". Each document in the collection should contain 
// two fields: a field called "_id" holding the city name, and a "users" field holding 
// an array of user_ids who currently live in that city. 
// Example: each document has following schema:
/*
{
  _id: city
  users:[userids]
}
*/

function cities_table(dbname) {
    db = db.getSiblingDB(dbname);
    // TODO: implemente cities collection here


    // Returns nothing. Instead, it creates a collection inside the datbase.

}
