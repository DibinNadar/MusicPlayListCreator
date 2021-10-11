package music;

import music.model.Artist;
import music.model.SongArtist;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Datasource {

    public static final String DB_NAME = "music.db";

    public static final String CONNECTION_STRING = "jdbc:sqlite:G:\\Projects\\Java\\MusicPlayListCreator\\scr\\musicDB\\"+DB_NAME;

    public static final String TABLE_ALBUMS = "albums";
    public static final String COLUMN_ALBUM_ID = "_id", COLUMN_ALBUM_NAME = "name", COLUMN_ALBUM_ARTIST = "artist";
    public static final int INDEX_ALBUM_ID = 1, INDEX_ALBUM_NAME = 2, INDEX_ALBUM_ARTIST = 3;

    public static final String TABLE_ARTISTS = "artists";
    public static final String COLUMN_ARTIST_ID = "_id", COLUMN_ARTIST_NAME = "name";
    public static final int INDEX_ARTIST_ID = 1, INDEX_ARTIST_NAME = 2;

    public static final String TABLE_SONGS = "songs";
    public static final String COLUMN_SONG_ID = "_id", COLUMN_SONG_TRACK = "track", COLUMN_SONG_TITLE = "title", COLUMN_SONG_ALBUM = "album";
    public static final int INDEX_SONG_ID = 1, INDEX_SONG_TRACK = 2, INDEX_SONG_TITLE = 3, INDEX_SONG_ALBUM = 4;

    public static final int ORDER_BY_NONE = 1, ORDER_BY_ASC  = 2, ORDER_BY_DESC = 3;

    public static final String QUERY_ALBUMS_BY_ARTIST_START =
        "SELECT " + TABLE_ALBUMS  + '.' + COLUMN_ALBUM_NAME   + " FROM " + TABLE_ALBUMS  + " INNER JOIN "+ TABLE_ARTISTS + " ON " +
                    TABLE_ALBUMS  + '.' + COLUMN_ALBUM_ARTIST + " = "    + TABLE_ARTISTS + '.' + COLUMN_ARTIST_ID + " WHERE " +
                    TABLE_ARTISTS + '.' + COLUMN_ARTIST_NAME  + " = \"" ;  // Remember to add closing "

    public static final String QUERY_ALBUMS_BY_ARTIST_SORT =
        " ORDER BY " + TABLE_ALBUMS + '.' +COLUMN_ALBUM_NAME + " COLLATE NOCASE ";

    public static final String QUERY_ARTIST_FOR_SONG_START =
        " SELECT " + TABLE_ARTISTS + '.' + COLUMN_ARTIST_NAME + ", " + TABLE_ALBUMS + '.' + COLUMN_ALBUM_NAME + ", " +
          TABLE_SONGS + '.' + COLUMN_SONG_TRACK + " FROM " + TABLE_SONGS + " INNER JOIN " + TABLE_ALBUMS + " ON " +
          TABLE_SONGS + '.' + COLUMN_SONG_ALBUM + " = " + TABLE_ALBUMS + '.' + COLUMN_ALBUM_ID + " INNER JOIN " +
          TABLE_ARTISTS + " ON " + TABLE_ALBUMS + '.' + COLUMN_ALBUM_ARTIST + " = " + TABLE_ARTISTS + '.' + COLUMN_ARTIST_ID +
          " WHERE " + TABLE_SONGS + '.' + COLUMN_SONG_TITLE + " = \"" ;

    public static final String QUERY_ARTIST_FOR_SONG_SORT =
        " ORDER BY " + TABLE_ARTISTS + '.' + COLUMN_ARTIST_NAME + ", " + TABLE_ALBUMS + '.' + COLUMN_ALBUM_NAME + " COLLATE NOCASE ";

    public static final String TABLE_ARTIST_SONG_VIEW = "artist_list";
    public static final String CREATE_ARTIST_FOR_SONG_VIEW = "CREATE VIEW IF NOT EXISTS "+
            TABLE_ARTIST_SONG_VIEW +" AS SELECT artists.name, albums.name AS album, songs.track, songs.title " +
            "FROM songs INNER JOIN albums ON songs.album = albums._id INNER JOIN artists ON albums.artist = " +
            "artists._id ORDER BY artists.name, albums.name, songs.track";



    public static final String QUERY_VIEW_SONG_INFO = "SELECT name, album, track FROM artist_list WHERE title = \"";



    private Connection connection;

    public boolean open(){
        try{
            connection = DriverManager.getConnection(CONNECTION_STRING);
            return true;
        }catch (SQLException e){
            System.out.printf("Could not connect to the DataBase: "+e.getMessage());
            return false;
        }
    }

    public void close(){
        try {
            if (connection != null){
                connection.close();   // Try would resources would eliminate this but we are trying to enforce the habit of closing
            }
        }catch (SQLException e){
            System.out.printf("Connection was not closed :" + e.getMessage());
        }
    }

    //   Try with Resources + Index not names

    public List<Artist> queryArtist(int sortOrder) {

        StringBuilder sb = new StringBuilder("SELECT * FROM ");
        sb.append(TABLE_ARTISTS);
        if (sortOrder != ORDER_BY_NONE){
            sb.append(" ORDER BY " + COLUMN_ARTIST_NAME + " COLLATE NOCASE ");
            if (sortOrder == ORDER_BY_DESC){
                sb.append("DESC");
            }else {
                sb.append("ASC");  // If any other value is passed, it will default to this
            }
        }

        try (Statement statement = connection.createStatement();
             ResultSet results = statement.executeQuery(sb.toString())) {

            List<Artist> artists = new ArrayList<>();
            while (results.next()) {
                Artist artist = new Artist();
                artist.setId(results.getInt(INDEX_ARTIST_ID));
                artist.setName(results.getString(INDEX_ARTIST_NAME));
                artists.add(artist);
            }
            return artists;

        } catch (SQLException e) {
            System.out.println("Query Failed : " + e.getMessage());
            return null;
        }

    }

    public List<String> queryAlbumsForArtists (String artistName, int sortOrder ){  // Returns only the Album names

        StringBuilder sb = new StringBuilder(QUERY_ALBUMS_BY_ARTIST_START );
        sb.append(artistName + "\"");

        if (sortOrder != ORDER_BY_NONE){
            sb.append(QUERY_ALBUMS_BY_ARTIST_SORT);
            if (sortOrder == ORDER_BY_DESC){
                sb.append("DESC");
            }else {
                sb.append("ASC");  // If any other value is passed, it will default to this
            }
        }
        System.out.println("SQL Statement : "+ sb); // Initial Debugging

        try (Statement statement = connection.createStatement();
             ResultSet results   = statement.executeQuery(sb.toString())) {
                List<String> albums = new ArrayList<>();
                while (results.next()){
                    albums.add(results.getString(1)); // This 1 is for the RESULT table, Hence HardCoding
                }

                return albums;

        } catch (SQLException e) {
            System.out.println("Query Failed : " + e.getMessage());
            return null;
        }
    }


    public List<SongArtist> queryArtistsForSong (String songName, int sortOrder){  // Returns only the Song names

        StringBuilder sb = new StringBuilder(QUERY_ARTIST_FOR_SONG_START );
        sb.append(songName + "\"");

        if (sortOrder != ORDER_BY_NONE){
            sb.append(QUERY_ALBUMS_BY_ARTIST_SORT);
            if (sortOrder == ORDER_BY_DESC){
                sb.append("DESC");
            }else {
                sb.append("ASC");
            }
        }
        System.out.println("SQL Statement : "+ sb);

        try (Statement statement = connection.createStatement();
             ResultSet results   = statement.executeQuery(sb.toString())) {
            List<SongArtist> songArtists = new ArrayList<>();
            while (results.next()){
                SongArtist songArtist = new SongArtist();
                songArtist.setArtistName(results.getString(1));
                songArtist.setAlbumName(results.getString(2));
                songArtist.setTrackNo(results.getInt(3));

                songArtists.add(songArtist);
            }

            return songArtists;

        } catch (SQLException e) {
            System.out.println("Query Failed : " + e.getMessage());
            return null;
        }
    }

    public void querySongsMetadata(){
        String sql = "SELECT * FROM " + TABLE_SONGS ;

        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql) ){

            ResultSetMetaData meta = resultSet.getMetaData();  // getMetaData used to get the schema
            int numColumns = meta.getColumnCount();

            for (int i=1; i<=numColumns; i++){
                System.out.format("Column %d in the songs table is %s \n",
                                  i, meta.getColumnName(i));
            }

        }catch (SQLException e){
            System.out.println("Query Failed "+ e.getMessage());
        }
    }

    public int getCount (String table) {
        String sql = "SELECT COUNT(*) AS count, MIN(_id) AS min_id FROM " + table;  // Error with MIN
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)){

                int count = resultSet.getInt("count");
                int min = resultSet.getInt("min_id");
            System.out.format("Count = %d, Min = %d\n", count, min);
                return count;
        } catch (SQLException e){
            System.out.println("Query Failed "+e.getMessage());
            return -1;
        }
    }

    public boolean createViewForSongArtists (){
        try (Statement statement = connection.createStatement()){

            statement.execute(CREATE_ARTIST_FOR_SONG_VIEW);
            System.out.println("View Created successfully!");
            return true;
        }catch (SQLException e){
            System.out.println("Create View Failed "+e.getMessage());
            return false;
        }
    }


    public List<SongArtist> querySongInfoView(String title) {
        StringBuilder sb = new StringBuilder(QUERY_VIEW_SONG_INFO);
        sb.append(title);
        sb.append("\"");

        System.out.println(sb);

        try (Statement statement = connection.createStatement();
             ResultSet results   = statement.executeQuery(sb.toString())) {

            ArrayList songArtists = new ArrayList();

            while (results.next()) {
                SongArtist songArtist = new SongArtist();
                songArtist.setArtistName(results.getString(1));
                songArtist.setAlbumName(results.getString(2));
                songArtist.setTrackNo(results.getInt(3));
                songArtists.add(songArtist);
            }
            return songArtists;
        }catch (SQLException e) {
            System.out.println("Query failed: " + e.getMessage());
            return null;
        }
    }
}
