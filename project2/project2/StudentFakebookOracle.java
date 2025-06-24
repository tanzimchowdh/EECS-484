package project2;

import javax.xml.transform.Result;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;

/*
    The StudentFakebookOracle class is derived from the FakebookOracle class and implements
    the abstract query functions that investigate the database provided via the <connection>
    parameter of the constructor to discover specific information.
*/
public final class StudentFakebookOracle extends FakebookOracle {
    // [Constructor]
    // REQUIRES: <connection> is a valid JDBC connection
    public StudentFakebookOracle(Connection connection) {
        oracle = connection;
    }
    
    @Override
    // Query 0
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the total number of users for which a birth month is listed
    //        (B) Find the birth month in which the most users were born
    //        (C) Find the birth month in which the fewest users (at least one) were born
    //        (D) Find the IDs, first names, and last names of users born in the month
    //            identified in (B)
    //        (E) Find the IDs, first names, and last name of users born in the month
    //            identified in (C)
    //
    // This query is provided to you completed for reference. Below you will find the appropriate
    // mechanisms for opening up a statement, executing a query, walking through results, extracting
    // data, and more things that you will need to do for the remaining nine queries
    public BirthMonthInfo findMonthOfBirthInfo() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            // Step 1
            // ------------
            // * Find the total number of users with birth month info
            // * Find the month in which the most users were born
            // * Find the month in which the fewest (but at least 1) users were born
            ResultSet rst = stmt.executeQuery(
                "SELECT COUNT(*) AS Birthed, Month_of_Birth " +         // select birth months and number of uses with that birth month
                "FROM " + UsersTable + " " +                            // from all users
                "WHERE Month_of_Birth IS NOT NULL " +                   // for which a birth month is available
                "GROUP BY Month_of_Birth " +                            // group into buckets by birth month
                "ORDER BY Birthed DESC, Month_of_Birth ASC");           // sort by users born in that month, descending; break ties by birth month
            
            int mostMonth = 0;
            int leastMonth = 0;
            int total = 0;
            while (rst.next()) {                       // step through result rows/records one by one
                if (rst.isFirst()) {                   // if first record
                    mostMonth = rst.getInt(2);         //   it is the month with the most
                }
                if (rst.isLast()) {                    // if last record
                    leastMonth = rst.getInt(2);        //   it is the month with the least
                }
                total += rst.getInt(1);                // get the first field's value as an integer
            }
            BirthMonthInfo info = new BirthMonthInfo(total, mostMonth, leastMonth);
            
            // Step 2
            // ------------
            // * Get the names of users born in the most popular birth month
            rst = stmt.executeQuery(
                "SELECT User_ID, First_Name, Last_Name " +                // select ID, first name, and last name
                "FROM " + UsersTable + " " +                              // from all users
                "WHERE Month_of_Birth = " + mostMonth + " " +             // born in the most popular birth month
                "ORDER BY User_ID");                                      // sort smaller IDs first
                
            while (rst.next()) {
                info.addMostPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 3
            // ------------
            // * Get the names of users born in the least popular birth month
            rst = stmt.executeQuery(
                "SELECT User_ID, First_Name, Last_Name " +                // select ID, first name, and last name
                "FROM " + UsersTable + " " +                              // from all users
                "WHERE Month_of_Birth = " + leastMonth + " " +            // born in the least popular birth month
                "ORDER BY User_ID");                                      // sort smaller IDs first
                
            while (rst.next()) {
                info.addLeastPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 4
            // ------------
            // * Close resources being used
            rst.close();
            stmt.close();                            // if you close the statement first, the result set gets closed automatically

            return info;

        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new BirthMonthInfo(-1, -1, -1);
        }
    }
    
    @Override
    // Query 1
    // -----------------------------------------------------------------------------------
    // GOALS: (A) The first name(s) with the most letters
    //        (B) The first name(s) with the fewest letters
    //        (C) The first name held by the most users
    //        (D) The number of users whose first name is that identified in (C)
    public FirstNameInfo findNameInfo() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                FirstNameInfo info = new FirstNameInfo();
                info.addLongName("Aristophanes");
                info.addLongName("Michelangelo");
                info.addLongName("Peisistratos");
                info.addShortName("Bob");
                info.addShortName("Sue");
                info.addCommonName("Harold");
                info.addCommonName("Jessica");
                info.setCommonNameCount(42);
                return info;
            */
            //return new FirstNameInfo();                // placeholder for compilation
            // Find most common name
            ResultSet rst = stmt.executeQuery(
                "SELECT FIRST_NAME, COUNT(FIRST_NAME) " +
                "FROM " + UsersTable + " " +
                "GROUP BY FIRST_NAME " +
                "HAVING COUNT(FIRST_NAME) >= ALL(SELECT COUNT(FIRST_NAME) FROM " +
                UsersTable + " GROUP BY FIRST_NAME)");
            long commonCount = 0;
            FirstNameInfo info = new FirstNameInfo();
            while (rst.next()) {
                if (rst.isFirst()){
                    commonCount = rst.getLong(2);
                }
                info.addCommonName(rst.getString(1));
            }
            info.setCommonNameCount(commonCount);

            // Get shortest names
            rst = stmt.executeQuery(
                "SELECT FIRST_NAME " +
                "FROM " + UsersTable + " " +
                "GROUP BY FIRST_NAME " +
                "HAVING LENGTH(FIRST_NAME) <= ALL(SELECT LENGTH(FIRST_NAME) FROM " +
                UsersTable + " GROUP BY FIRST_NAME) " +
                "ORDER BY FIRST_NAME ASC");
            while (rst.next()) {
                info.addShortName(rst.getString(1));
            }

            // Get longest names
            rst = stmt.executeQuery(
                "SELECT FIRST_NAME " +
                    "FROM " + UsersTable + " " +
                    "GROUP BY FIRST_NAME " +
                    "HAVING LENGTH(FIRST_NAME) >= ALL(SELECT LENGTH(FIRST_NAME) FROM " +
                    UsersTable + " GROUP BY FIRST_NAME) " +
                    "ORDER BY FIRST_NAME ASC");
            while (rst.next()) {
                info.addLongName(rst.getString(1));
            }

            // Close everything
            rst.close();
            stmt.close();                            // if you close the statement first, the result set gets closed automatically

            return info;
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new FirstNameInfo();
        }
    }
    
    @Override
    // Query 2
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of users without any friends
    //
    // Be careful! Remember that if two users are friends, the Friends table only contains
    // the one entry (U1, U2) where U1 < U2.
    public FakebookArrayList<UserInfo> lonelyUsers() throws SQLException {
        FakebookArrayList<UserInfo> results = new FakebookArrayList<UserInfo>(", ");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(15, "Abraham", "Lincoln");
                UserInfo u2 = new UserInfo(39, "Margaret", "Thatcher");
                results.add(u1);
                results.add(u2);
            */
//            ResultSet rst = stmt.executeQuery(
//                "SELECT USER_ID, FIRST_NAME, LAST_NAME " +
//                    "FROM " + UsersTable + "U " +
//                    "WHERE NOT EXISTS (SELECT * FROM " + FriendsTable + " WHERE USER1_ID = U.USER_ID OR USER2_ID = U.USER_ID) " +
//                    "ORDER BY USER_ID ASC");
            ResultSet rst = stmt.executeQuery(
                "SELECT USER_ID, FIRST_NAME, LAST_NAME " +
                "FROM " + UsersTable + " U LEFT JOIN " + FriendsTable + " F1 ON (U.USER_ID = F1.USER1_ID) LEFT JOIN " + FriendsTable + " F2 ON (U.USER_ID = F2.USER2_ID) " +
                "WHERE F1.USER1_ID IS NULL AND F2.USER2_ID IS NULL " +
                "ORDER BY USER_ID ASC");
            while (rst.next()) {
                results.add(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Close everything
            rst.close();
            stmt.close();                            // if you close the statement first, the result set gets closed automatically

        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    @Override
    // Query 3
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of users who no longer live
    //            in their hometown (i.e. their current city and their hometown are different)
    public FakebookArrayList<UserInfo> liveAwayFromHome() throws SQLException {
        FakebookArrayList<UserInfo> results = new FakebookArrayList<UserInfo>(", ");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(9, "Meryl", "Streep");
                UserInfo u2 = new UserInfo(104, "Tom", "Hanks");
                results.add(u1);
                results.add(u2);
            */
            ResultSet rst = stmt.executeQuery(
                "SELECT U.USER_ID, U.FIRST_NAME, U.LAST_NAME " +
                "FROM " + UsersTable + " U JOIN " + CurrentCitiesTable + " C ON (U.USER_ID = C.USER_ID) JOIN " + HometownCitiesTable + " H ON (U.USER_ID = H.USER_ID) " +
                "WHERE C.CURRENT_CITY_ID <> H.HOMETOWN_CITY_ID " +
                "ORDER BY U.USER_ID ASC");
            if (!rst.next()) {
                return results;
            }
            results.add(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            while (rst.next()) {
                results.add(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Close everything
            rst.close();
            stmt.close();                            // if you close the statement first, the result set gets closed automatically

        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    @Override
    // Query 4
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, links, and IDs and names of the containing album of the top
    //            <num> photos with the most tagged users
    //        (B) For each photo identified in (A), find the IDs, first names, and last names
    //            of the users therein tagged
    public FakebookArrayList<TaggedPhotoInfo> findPhotosWithMostTags(int num) throws SQLException {
        FakebookArrayList<TaggedPhotoInfo> results = new FakebookArrayList<TaggedPhotoInfo>("\n");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly);
             Statement stmt2 = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                PhotoInfo p = new PhotoInfo(80, 5, "www.photolink.net", "Winterfell S1");
                UserInfo u1 = new UserInfo(3901, "Jon", "Snow");
                UserInfo u2 = new UserInfo(3902, "Arya", "Stark");
                UserInfo u3 = new UserInfo(3903, "Sansa", "Stark");
                TaggedPhotoInfo tp = new TaggedPhotoInfo(p);
                tp.addTaggedUser(u1);
                tp.addTaggedUser(u2);
                tp.addTaggedUser(u3);
                results.add(tp);
            */
            ResultSet rst = stmt.executeQuery(
                "SELECT P.PHOTO_ID, P.ALBUM_ID, P.PHOTO_LINK, A.ALBUM_NAME, (SELECT COUNT(*) FROM " + TagsTable + " T WHERE P.PHOTO_ID = T.TAG_PHOTO_ID) AS TAG_COUNT " +
                "FROM " + PhotosTable + " P JOIN " + AlbumsTable + " A ON (P.ALBUM_ID = A.ALBUM_ID) " +
                "ORDER BY TAG_COUNT DESC, P.PHOTO_ID ASC " +
                "FETCH FIRST " + num + " ROWS ONLY");

            while (rst.next()) {
                if (rst.getInt(5) == 0) {
                    break;
                }
                TaggedPhotoInfo tempTag = new TaggedPhotoInfo(new PhotoInfo(rst.getLong(1), rst.getLong(2), rst.getString(3), rst.getString(4)));
                ResultSet taggedUsers = stmt2.executeQuery(
                    "SELECT U.USER_ID, U.FIRST_NAME, U.LAST_NAME " +
                        "FROM " + UsersTable + " U JOIN " + TagsTable + " T ON (U.USER_ID = T.TAG_SUBJECT_ID) " +
                        "WHERE T.TAG_PHOTO_ID = " + rst.getString(1) + " " +
                        "ORDER BY U.USER_ID ASC");
                while (taggedUsers.next()) {
                    tempTag.addTaggedUser(new UserInfo(taggedUsers.getLong(1), taggedUsers.getString(2), taggedUsers.getString(3)));
                }
                results.add(tempTag);
            }
            rst.close();
            stmt.close();                            // if you close the statement first, the result set gets closed automatically
            stmt2.close();

        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    @Override
    // Query 5
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, last names, and birth years of each of the two
    //            users in the top <num> pairs of users that meet each of the following
    //            criteria:
    //              (i) same gender
    //              (ii) tagged in at least one common photo
    //              (iii) difference in birth years is no more than <yearDiff>
    //              (iv) not friends
    //        (B) For each pair identified in (A), find the IDs, links, and IDs and names of
    //            the containing album of each photo in which they are tagged together
    public FakebookArrayList<MatchPair> matchMaker(int num, int yearDiff) throws SQLException {
        FakebookArrayList<MatchPair> results = new FakebookArrayList<MatchPair>("\n");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly);
             Statement stmt2 = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(93103, "Romeo", "Montague");
                UserInfo u2 = new UserInfo(93113, "Juliet", "Capulet");
                MatchPair mp = new MatchPair(u1, 1597, u2, 1597);
                PhotoInfo p = new PhotoInfo(167, 309, "www.photolink.net", "Tragedy");
                mp.addSharedPhoto(p);
                results.add(mp);
            */
            ResultSet rst = stmt.executeQuery(
                "SELECT U1.USER_ID, U1.FIRST_NAME, U1.LAST_NAME, U1.YEAR_OF_BIRTH, U2.USER_ID, U2.FIRST_NAME, U2.LAST_NAME, U2.YEAR_OF_BIRTH, (SELECT COUNT(*) FROM " + TagsTable + " T1 JOIN " + TagsTable + " T2 ON (T1.TAG_PHOTO_ID = T2.TAG_PHOTO_ID AND T1.TAG_SUBJECT_ID = U1.USER_ID AND T2.TAG_SUBJECT_ID = U2.USER_ID)) AS TAG_COUNT " +
                    "FROM " + UsersTable + " U1 JOIN " + UsersTable + " U2 ON (U1.USER_ID < U2.USER_ID AND U1.GENDER = U2.GENDER AND (ABS(U1.YEAR_OF_BIRTH - U2.YEAR_OF_BIRTH) <= " + yearDiff + ")) " +
                    "WHERE NOT EXISTS (SELECT * FROM " + FriendsTable + " WHERE USER1_ID = U1.USER_ID AND USER2_ID = U2.USER_ID) " +
                    "ORDER BY TAG_COUNT DESC, U1.USER_ID ASC, U2.USER_ID ASC " +
                    "FETCH FIRST " + num + " ROWS ONLY");
            long user1_ID;
            long user2_ID;
            while (rst.next()) {
                if (rst.getLong(9) != 0) {
                    user1_ID = rst.getLong(1);
                    user2_ID = rst.getLong(5);
                    MatchPair temp = new MatchPair(new UserInfo(user1_ID, rst.getString(2), rst.getString(3)), rst.getLong(4), new UserInfo(user2_ID, rst.getString(6), rst.getString(7)), rst.getLong(8));
                    ResultSet photos = stmt2.executeQuery(
                        "SELECT P.PHOTO_ID, P.PHOTO_LINK, P.ALBUM_ID, A.ALBUM_NAME " +
                            "FROM " + TagsTable + " T1 JOIN " + TagsTable + " T2 ON (T1.TAG_PHOTO_ID = T2.TAG_PHOTO_ID AND T1.TAG_SUBJECT_ID = " + user1_ID + " AND T2.TAG_SUBJECT_ID = " + user2_ID + ") JOIN " + PhotosTable + " P ON (T1.TAG_PHOTO_ID = P.PHOTO_ID) JOIN " + AlbumsTable + " A ON (P.ALBUM_ID = A.ALBUM_ID) " +
                            "ORDER BY P.PHOTO_ID ASC");
                    while (photos.next()) {
                        temp.addSharedPhoto(new PhotoInfo(photos.getLong(1), photos.getLong(3), photos.getString(2), photos.getString(4)));
                    }
                    results.add(temp);
                }
            }
            rst.close();
            stmt.close();                            // if you close the statement first, the result set gets closed automatically
            stmt2.close();
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    @Override
    // Query 6
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of each of the two users in
    //            the top <num> pairs of users who are not friends but have a lot of
    //            common friends
    //        (B) For each pair identified in (A), find the IDs, first names, and last names
    //            of all the two users' common friends
    public FakebookArrayList<UsersPair> suggestFriends(int num) throws SQLException {
        FakebookArrayList<UsersPair> results = new FakebookArrayList<UsersPair>("\n");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(16, "The", "Hacker");
                UserInfo u2 = new UserInfo(80, "Dr.", "Marbles");
                UserInfo u3 = new UserInfo(192, "Digit", "Le Boid");
                UsersPair up = new UsersPair(u1, u2);
                up.addSharedFriend(u3);
                results.add(up);
            */
            ResultSet rst = stmt.executeQuery(
                "SELECT MUTFRIENDS.PERSON_1, U1.FIRST_NAME, U1.LAST_NAME, MUTFRIENDS.PERSON_2, U2.FIRST_NAME, U2.LAST_NAME, MUTFRIENDS.PERSON_3, U3.FIRST_NAME, U3.LAST_NAME, MUTFRIENDS.PERSON_COUNT " +
                "FROM (" +
                 "SELECT MUTTABLE.P1 AS PERSON_1, MUTTABLE.P2 AS PERSON_2, COUNT(*) AS PERSON_COUNT FROM (" +
                    "SELECT F1.USER1_ID AS P1, F2.USER2_ID AS P2, F2.USER1_ID AS P3 FROM " + FriendsTable + " F1 JOIN " + FriendsTable + "F2 ON (F1.USER2_ID = F2.USER1_ID AND F1.USER1_ID != F2.USER2_ID) " +
                    "UNION ALL SELECT F1.USER1_ID AS P1, F2.USER1_ID AS P2, F2.USER2_ID AS P3 FROM " + FriendsTable + " F1 JOIN " + FriendsTable + "F2 ON (F1.USER2_ID = F2.USER2_ID AND F1.USER1_ID != F2.USER1_ID) " +
                    "UNION ALL SELECT F1.USER2_ID AS P1, F2.USER2_ID AS P2, F2.USER1_ID AS P3 FROM " + FriendsTable + " F1 JOIN " + FriendsTable + "F2 ON (F1.USER1_ID = F2.USER1_ID AND F1.USER2_ID != F2.USER2_ID) ORDER BY 3 ASC) MUTTABLE " +
                     "GROUP BY MUTTABLE.P1, MUTTABLE.P2 ORDER BY 3 DESC" +
                    ") MUTFRIENDS FULL OUTER JOIN (" +
                    "SELECT F1.USER1_ID AS PERSON_1, F2.USER2_ID AS PERSON_2, F2.USER1_ID AS PERSON_3 FROM "+ FriendsTable + " F1 JOIN " + FriendsTable + " F2 ON (F1.USER2_ID = F2.USER1_ID AND F1.USER1_ID != F2.USER2_ID)" +
                    "UNION ALL SELECT F1.USER1_ID, F2.USER1_ID, F2.USER2_ID FROM "+ FriendsTable + " F1 JOIN " + FriendsTable + " F2 ON (F1.USER2_ID = F2.USER2_ID AND F1.USER1_ID != F2.USER1_ID)" +
                    "UNION ALL SELECT F1.USER2_ID, F2.USER2_ID, F2.USER1_ID FROM "+ FriendsTable + " F1 JOIN " + FriendsTable + " F2 ON (F1.USER1_ID = F2.USER1_ID AND F1.USER2_ID != F2.USER2_ID)" +
                    ") MUTLIST ON MUTFRIENDS.PERSON_1 = MUTLIST.PERSON_1 AND MUTFRIENDS.PERSON_2 = MUTLIST.PERSON_2 " +
                    "FULL OUTER JOIN "+ UsersTable + " U1 ON U1.USER_ID = MUTFRIENDS.PERSON_1 " +
                    "FULL OUTER JOIN "+ UsersTable + " U2 ON U2.USER_ID = MUTFRIENDS.PERSON_2 " +
                    "FULL OUTER JOIN "+ UsersTable + " U3 ON U3.USER_ID = MUTFRIENDS.PERSON_3 " +
                    "WHERE MUTFRIENDS.PERSON_1 < MUTFRIENDS.PERSON_2 AND MUTFRIENDS.PERSON_1 NOT IN " +
                    "(SELECT F.USER1_ID FROM " + FriendsTable + " F WHERE (MUTFRIENDS.PERSON_1 = F.USER1_ID AND MUTFRIENDS.PERSON_2 = F.USER2_ID)) " +
                    "ORDER BY MUTFRIENDS.PERSON_COUNT DESC, MUTFRIENDS.PERSON_1 ASC, MUTFRIENDS.PERSON_2 ASC, MUTFRIENDS.PERSON_3 ASC");
            long user1_id;
            long user2_id;
            rst.next();
            for(int i = 0; i < num; ++i) {
                long temp1_ID = rst.getLong(1);
                String user1_FNAME = rst.getString(2);
                String user1_LNAME = rst.getString(2);
                long temp2_ID = rst.getLong(1);
                String user2_FNAME = rst.getString(2);
                String user2_LNAME = rst.getString(2);
                UsersPair temp = new UsersPair(new UserInfo(temp1_ID, user1_FNAME, user1_LNAME), new UserInfo(temp2_ID, user2_FNAME, user2_LNAME));
                user1_id = temp1_ID;
                user2_id = temp2_ID;
                do {
                    temp1_ID = rst.getLong(1);
                    temp2_ID = rst.getLong(4);
                    if (user1_id != temp1_ID || user2_id != temp2_ID) {
                        break;
                    }
                    long mutual_ID = rst.getLong(7);
                    String first_name = rst.getString(8);
                    String last_name = rst.getString(9);
                    temp.addSharedFriend(new UserInfo(mutual_ID, first_name, last_name));
                    results.add(temp);
                } while(rst.next());
            }
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    @Override
    // Query 7
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the name of the state or states in which the most events are held
    //        (B) Find the number of events held in the states identified in (A)
    public EventStateInfo findEventStates() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                EventStateInfo info = new EventStateInfo(50);
                info.addState("Kentucky");
                info.addState("Hawaii");
                info.addState("New Hampshire");
                return info;
            */
            ResultSet rst = stmt.executeQuery(
                "SELECT C.STATE_NAME, COUNT(C.STATE_NAME) " +
                    "FROM " + EventsTable + " E JOIN " + CitiesTable + " C ON (E.EVENT_CITY_ID = C.CITY_ID) " +
                    "GROUP BY C.STATE_NAME " +
                    "ORDER BY COUNT(C.STATE_NAME) DESC, C.STATE_NAME ASC");
            rst.next();
            long count = rst.getLong(2);
            long currentCount = count;
            EventStateInfo result = new EventStateInfo(count);
            String state = rst.getString(1);
            String currentState = state;
            result.addState(state);
            while(currentCount == count && rst.next()) {
                currentCount = rst.getLong(2);
                currentState = rst.getString(1);
                if (currentCount == count) {
                    if (currentState != state){
                        state = currentState;
                        result.addState(currentState);
                    }
                }
            }
            rst.close();
            stmt.close();                            // if you close the statement first, the result set gets closed automaticall
            return result;
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new EventStateInfo(-1);
        }
    }
    
    @Override
    // Query 8
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the ID, first name, and last name of the oldest friend of the user
    //            with User ID <userID>
    //        (B) Find the ID, first name, and last name of the youngest friend of the user
    //            with User ID <userID>
    public AgeInfo findAgeInfo(long userID) throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo old = new UserInfo(12000000, "Galileo", "Galilei");
                UserInfo young = new UserInfo(80000000, "Neil", "deGrasse Tyson");
                return new AgeInfo(old, young);
            */
            ResultSet rst = stmt.executeQuery(
                "SELECT U.USER_ID, U.FIRST_NAME, U.LAST_NAME " +
                    "FROM " + UsersTable + " U JOIN " + FriendsTable + " F ON ((F.USER1_ID = " + userID + " AND F.USER2_ID = U.USER_ID) OR (F.USER1_ID = U.USER_ID AND F.USER2_ID = " + userID + ")) " +
                    "ORDER BY U.YEAR_OF_BIRTH DESC, U.MONTH_OF_BIRTH DESC, U.DAY_OF_BIRTH DESC, U.USER_ID DESC " +
                    "FETCH FIRST 1 ROW ONLY");
            rst.next();
            UserInfo youngest = new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3));
            rst = stmt.executeQuery(
                "SELECT U.USER_ID, U.FIRST_NAME, U.LAST_NAME " +
                    "FROM " + UsersTable + " U JOIN " + FriendsTable + " F ON ((F.USER1_ID = " + userID + " AND F.USER2_ID = U.USER_ID) OR (F.USER1_ID = U.USER_ID AND F.USER2_ID = " + userID + ")) " +
                    "ORDER BY U.YEAR_OF_BIRTH ASC, U.MONTH_OF_BIRTH ASC, U.DAY_OF_BIRTH ASC, U.USER_ID DESC " +
                    "FETCH FIRST 1 ROW ONLY");
            rst.next();
            UserInfo oldest = new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3));
            rst.close();
            stmt.close();                            // if you close the statement first, the result set gets closed automatically
            return new AgeInfo(oldest, youngest);
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new AgeInfo(new UserInfo(-1, "ERROR", "ERROR"), new UserInfo(-1, "ERROR", "ERROR"));
        }
    }
    
    @Override
    // Query 9
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find all pairs of users that meet each of the following criteria
    //              (i) same last name
    //              (ii) same hometown
    //              (iii) are friends
    //              (iv) less than 10 birth years apart
    public FakebookArrayList<SiblingInfo> findPotentialSiblings() throws SQLException {
        FakebookArrayList<SiblingInfo> results = new FakebookArrayList<SiblingInfo>("\n");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(81023, "Kim", "Kardashian");
                UserInfo u2 = new UserInfo(17231, "Kourtney", "Kardashian");
                SiblingInfo si = new SiblingInfo(u1, u2);
                results.add(si);
            */
            ResultSet rst = stmt.executeQuery(
                "SELECT U1.USER_ID, U1.FIRST_NAME, U1.LAST_NAME, U2.USER_ID, U2.FIRST_NAME, U2.LAST_NAME " +
                    "FROM " + UsersTable + " U1 JOIN " + UsersTable + " U2 ON ((U1.USER_ID < U2.USER_ID) AND (U1.LAST_NAME = U2.LAST_NAME) AND (ABS(U1.YEAR_OF_BIRTH - U2.YEAR_OF_BIRTH) < 10)) JOIN " +
                    FriendsTable + " F ON ((F.USER1_ID = U1.USER_ID) AND (F.USER2_ID = U2.USER_ID)) JOIN " +
                    HometownCitiesTable + " H1 ON (U1.USER_ID = H1.USER_ID) JOIN " +
                    HometownCitiesTable + " H2 ON (U2.USER_ID = H2.USER_ID AND H1.HOMETOWN_CITY_ID = H2.HOMETOWN_CITY_ID) " +
                    "ORDER BY U1.USER_ID ASC, U2.USER_ID ASC");
            while (rst.next()) {
                results.add(new SiblingInfo(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)), new UserInfo(rst.getLong(4), rst.getString(5), rst.getString(6))));
            }
            rst.close();
            stmt.close();                            // if you close the statement first, the result set gets closed automatically
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    // Member Variables
    private Connection oracle;
    private final String UsersTable = FakebookOracleConstants.UsersTable;
    private final String CitiesTable = FakebookOracleConstants.CitiesTable;
    private final String FriendsTable = FakebookOracleConstants.FriendsTable;
    private final String CurrentCitiesTable = FakebookOracleConstants.CurrentCitiesTable;
    private final String HometownCitiesTable = FakebookOracleConstants.HometownCitiesTable;
    private final String ProgramsTable = FakebookOracleConstants.ProgramsTable;
    private final String EducationTable = FakebookOracleConstants.EducationTable;
    private final String EventsTable = FakebookOracleConstants.EventsTable;
    private final String AlbumsTable = FakebookOracleConstants.AlbumsTable;
    private final String PhotosTable = FakebookOracleConstants.PhotosTable;
    private final String TagsTable = FakebookOracleConstants.TagsTable;
}
