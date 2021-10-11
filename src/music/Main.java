package music;


import music.model.Artist;
import music.model.SongArtist;

import java.util.List;

public class Main {

    public static void main(String[] args) {

        Datasource datasource = new Datasource();

        if (!datasource.open()){
            System.out.println(" Datasource was not opened");
            return;
        }

        List<Artist> artists = datasource.queryArtist(Datasource.ORDER_BY_ASC); //
        if (artists == null){
            System.out.println("No artists present");
            return;
        }
        for (Artist artist : artists){
            System.out.println("ID = "+artist.getId()+", Name = "+artist.getName());
        }
        System.out.println();

        List<String> albumsForArtist = datasource.queryAlbumsForArtists("Iron Maiden",Datasource.ORDER_BY_ASC);
        for (String album : albumsForArtist){
            System.out.println(album);
        }
        System.out.println();

        List<SongArtist> songArtists = datasource.queryArtistsForSong("Go Your Own Way",Datasource.ORDER_BY_ASC);
        if (songArtists == null){
            System.out.println("Song not found");
            return;
        }
        for (SongArtist artist : songArtists){
            System.out.println("\nArtist Name = "+artist.getArtistName()+"\nAlbum Name  = "+artist.getAlbumName()+"\nTrack No    = "+artist.getTrackNo());
        }
        System.out.println();

        datasource.querySongsMetadata();
        System.out.println();

        int count = datasource.getCount(Datasource.TABLE_SONGS);
        System.out.println("Number of songs is : "+count);
        System.out.println();

        datasource.createViewForSongArtists();
        System.out.println();

        songArtists = datasource.querySongInfoView("Go Your Own Way");
        if (songArtists.isEmpty()){
            System.out.println("Song not available");
            return;
        }

        for (SongArtist artist : songArtists){
            System.out.println("FROM VIEW - Artist name = " + artist.getArtistName() +
                    " Album Name = " + artist.getAlbumName() +
                    " Track Number = " + artist.getTrackNo());
        }

        datasource.close();


    }
}

