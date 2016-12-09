package master2016.twitterApp;


import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Created by Sophie on 11/18/16.
 */
public class TwitterParser {

    public String parse(String jsonString) {
        try{
//            System.out.println(jsonString);

            JsonElement jsonElement = new JsonParser().parse(jsonString);

            JsonObject jsonObject = jsonElement.getAsJsonObject();
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
                // empty instead of null, for sometimes there is hashtag null
                hashTags += ":";
            }

            return  language + hashTags;

        }
        catch(Exception e) { // catch exception when language or hashtags does not exist
            e.printStackTrace();
        }

        // empty instead of null when no language or hashtasg is defined, for sometimes there is hashtag null
        return "lang:,hashtags:";
    }

}
