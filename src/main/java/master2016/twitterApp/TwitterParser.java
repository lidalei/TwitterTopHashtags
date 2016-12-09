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

            String hashTag = ",hashtag";
            if(hashTagArr.size() >= 1) {
                for(JsonElement e : hashTagArr) {
                    hashTag += ":" + e.getAsJsonObject().get("text").getAsString();
                }
            }
            else {
                hashTag += ":null";
            }

            return  language + hashTag;

        }
        catch(Exception e) { // catch exception when language or hashtag does not exist
            e.printStackTrace();
        }

        return "lang:null,hashtag:null";
    }

}
