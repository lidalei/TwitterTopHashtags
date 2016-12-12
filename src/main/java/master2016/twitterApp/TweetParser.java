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
//            System.out.println(jsonString);

            JsonElement jsonElement = new JsonParser().parse(jsonString);

            JsonObject jsonObject = jsonElement.getAsJsonObject();

            // the twitter was deleted if there is no language element, TODO
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
            }
            else {
                // empty instead of null, for sometimes there is hashtag null, TODO, drop or keep?
                hashTags += ":";
            }

            return  language + hashTags;

        }
        catch(Exception e) { // catch exception when language or hashtags does not exist
            e.printStackTrace();
        }

        // null when no language or hashtasg is defined
        return null;
    }

}
