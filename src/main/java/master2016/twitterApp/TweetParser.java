package master2016.twitterApp;


import com.google.gson.*;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * Created by Sophie on 11/18/16.
 */
public class TweetParser {

    private final static JsonParser jsonParser = new JsonParser();

    // TODO, change after finishing development
    private BufferedWriter writer = null;

    // TODO, change after finishing development
    public TweetParser() {
        try{
            writer = new BufferedWriter(new OutputStreamWriter( new FileOutputStream("/Users/Sophie/sampleTwitters.txt"), StandardCharsets.UTF_8));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }


    public String parse(String jsonString) {
        try{
            // TODO, change after finishing development
            writer.write(jsonString);
        }
        catch(Exception e) { // catch exception when language or hashtags does not exist
            e.printStackTrace();
        }

        // System.out.println(jsonString);

        JsonElement jsonElement = jsonParser.parse(jsonString);

        JsonObject jsonObject = jsonElement.getAsJsonObject();

        // the tweet was deleted if there is no language element
//            System.out.println(jsonObject.get("lang"));
        JsonElement langJsonObject = jsonObject.get("lang");

        if(langJsonObject == null) {
            return null;
        }

        String language = langJsonObject.getAsString();

        String hashTags = ",";
        JsonArray hashTagArr = jsonObject.getAsJsonObject("entities").getAsJsonArray("hashtags");

        if(hashTagArr.size() >= 1) {
            for(JsonElement e : hashTagArr) {
                // transform hashtag to lower case format
                hashTags += e.getAsJsonObject().get("text").getAsString().toLowerCase() + ":";
            }

            String languageHashTags = language + hashTags;
            return  languageHashTags.substring(0, languageHashTags.length() - 1);
        }
        else {
            // there is no hashtag, directly drop the tweet
            return null;
        }
    }

}
