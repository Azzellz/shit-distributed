package com.example;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Movie implements WritableComparable<Movie> {
    private String genres;
    private String title;
    private int year;
    private float rating;

    public Movie() {
    }

    public Movie(String genres, String title, int year, float rating) {
        this.genres = genres;
        this.title = title;
        this.year = year;
        this.rating = rating;
    }

    public void set(String genres, String title, int year, float rating) {
        this.genres = genres;
        this.title = title;
        this.year = year;
        this.rating = rating;
    }

    // Implement methods of the WritableComparable interface
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(genres);
        out.writeUTF(title);
        out.writeInt(year);
        out.writeFloat(rating);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        genres = in.readUTF();
        title = in.readUTF();
        year = in.readInt();
        rating = in.readFloat();
    }

    // 比较三个参数
    @Override
    public int compareTo(Movie o) {
        int result = genres.compareTo(o.genres);
        if (result == 0) {
            result = year - o.year;
            if (result == 0) {
                result = Float.compare(rating, o.rating);
            }
        }
        return result;
    }

    // Redefine the hashcode and equals methods if you want to use this class as a key in a Map
    @Override
    public int hashCode() {
        // Custom hash function
        return genres.hashCode() + title.hashCode() + year + Float.hashCode(rating);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Movie movie = (Movie) obj;
        return year == movie.year && Float.compare(movie.rating, rating) == 0 && genres.equals(movie.genres) && title.equals(movie.title);
    }

    @Override
    public String toString() {
        return "Movie{" + "genres='" + genres + '\'' + ", title='" + title + '\'' + ", year=" + year + ", rating=" + rating + '}';
    }
}