package master2016.twitterApp;


import com.google.gson.*;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * Created by Sophie on 11/18/16.
 */
public class TweetParser {

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

        JsonElement jsonElement = new JsonParser().parse(jsonString);

        JsonObject jsonObject = jsonElement.getAsJsonObject();

        // the tweet was deleted if there is no language element
//            System.out.println(jsonObject.get("lang"));
        if(jsonObject.get("lang") == null) {
            return null;
        }

        String language = "lang:" + jsonObject.get("lang").getAsString();
        JsonArray hashTagArr = jsonObject.getAsJsonObject("entities").getAsJsonArray("hashtags");

        String hashTags = ",hashtags";
        if(hashTagArr.size() >= 1) {
            for(JsonElement e : hashTagArr) {
                // transform hashtag to lower case format
                hashTags += ":" + e.getAsJsonObject().get("text").getAsString().toLowerCase();
            }

            return  language + hashTags;
        }
        else {
            // empty instead of null, for sometimes there is hashtag null
            // hashTags += ":";
            // directly drop the tweet
            return null;
        }
    }

}
