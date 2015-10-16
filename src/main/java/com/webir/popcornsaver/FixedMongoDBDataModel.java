package com.webir.popcornsaver;//

import com.google.common.base.Preconditions;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import org.apache.mahout.cf.taste.common.NoSuchItemException;
import org.apache.mahout.cf.taste.common.NoSuchUserException;
import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.GenericDataModel;
import org.apache.mahout.cf.taste.impl.model.GenericPreference;
import org.apache.mahout.cf.taste.impl.model.GenericUserPreferenceArray;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
This class is a quick and dirty patch to MongoDBDataModel, which changes Mongo ids even
if they are long, contrary to what is stated in its documentation.
 */
public final class FixedMongoDBDataModel implements DataModel {
    private static final Logger log = LoggerFactory.getLogger(FixedMongoDBDataModel.class);
    private static final String DEFAULT_MONGO_HOST = "localhost";
    private static final int DEFAULT_MONGO_PORT = 27017;
    private static final String DEFAULT_MONGO_DB = "recommender";
    private static final boolean DEFAULT_MONGO_AUTH = false;
    private static final String DEFAULT_MONGO_USERNAME = "recommender";
    private static final String DEFAULT_MONGO_PASSWORD = "recommender";
    private static final String DEFAULT_MONGO_COLLECTION = "items";
    private static final boolean DEFAULT_MONGO_MANAGE = true;
    private static final String DEFAULT_MONGO_USER_ID = "user_id";
    private static final String DEFAULT_MONGO_ITEM_ID = "item_id";
    private static final String DEFAULT_MONGO_PREFERENCE = "preference";
    private static final boolean DEFAULT_MONGO_FINAL_REMOVE = false;
    private static final DateFormat DEFAULT_DATE_FORMAT;
    public static final String DEFAULT_MONGO_MAP_COLLECTION = "mongo_data_model_map";
    private static final Pattern ID_PATTERN;
    private String mongoHost = "localhost";
    private int mongoPort = 27017;
    private String mongoDB = "recommender";
    private boolean mongoAuth = false;
    private String mongoUsername = "recommender";
    private String mongoPassword = "recommender";
    private String mongoCollection = "items";
    private String mongoMapCollection = "mongo_data_model_map";
    private boolean mongoManage = true;
    private String mongoUserID = "user_id";
    private String mongoItemID = "item_id";
    private String mongoPreference = "preference";
    private boolean mongoFinalRemove = false;
    private DateFormat dateFormat;
    private DBCollection collection;
    private DBCollection collectionMap;
    private Date mongoTimestamp;
    private final ReentrantLock reloadLock;
    private DataModel delegate;
    private boolean userIsObject;
    private boolean itemIsObject;
    private boolean preferenceIsString;
    private long idCounter;

    public FixedMongoDBDataModel() throws UnknownHostException {
        this.dateFormat = DEFAULT_DATE_FORMAT;
        this.reloadLock = new ReentrantLock();
        this.buildModel();
    }

    public FixedMongoDBDataModel(String host, int port, String database, String collection, boolean manage, boolean finalRemove, DateFormat format) throws UnknownHostException {
        this.dateFormat = DEFAULT_DATE_FORMAT;
        this.mongoHost = host;
        this.mongoPort = port;
        this.mongoDB = database;
        this.mongoCollection = collection;
        this.mongoManage = manage;
        this.mongoFinalRemove = finalRemove;
        this.dateFormat = format;
        this.reloadLock = new ReentrantLock();
        this.buildModel();
    }

    public FixedMongoDBDataModel(String host, int port, String database, String collection, boolean manage, boolean finalRemove, DateFormat format, String userIDField, String itemIDField, String preferenceField, String mappingCollection) throws UnknownHostException {
        this.dateFormat = DEFAULT_DATE_FORMAT;
        this.mongoHost = host;
        this.mongoPort = port;
        this.mongoDB = database;
        this.mongoCollection = collection;
        this.mongoManage = manage;
        this.mongoFinalRemove = finalRemove;
        this.dateFormat = format;
        this.mongoUserID = userIDField;
        this.mongoItemID = itemIDField;
        this.mongoPreference = preferenceField;
        this.mongoMapCollection = mappingCollection;
        this.reloadLock = new ReentrantLock();
        this.buildModel();
    }

    public FixedMongoDBDataModel(String host, int port, String database, String collection, boolean manage, boolean finalRemove, DateFormat format, String user, String password) throws UnknownHostException {
        this.dateFormat = DEFAULT_DATE_FORMAT;
        this.mongoHost = host;
        this.mongoPort = port;
        this.mongoDB = database;
        this.mongoCollection = collection;
        this.mongoManage = manage;
        this.mongoFinalRemove = finalRemove;
        this.dateFormat = format;
        this.mongoAuth = true;
        this.mongoUsername = user;
        this.mongoPassword = password;
        this.reloadLock = new ReentrantLock();
        this.buildModel();
    }

    public FixedMongoDBDataModel(String host, int port, String database, String collection, boolean manage, boolean finalRemove, DateFormat format, String user, String password, String userIDField, String itemIDField, String preferenceField, String mappingCollection) throws UnknownHostException {
        this.dateFormat = DEFAULT_DATE_FORMAT;
        this.mongoHost = host;
        this.mongoPort = port;
        this.mongoDB = database;
        this.mongoCollection = collection;
        this.mongoManage = manage;
        this.mongoFinalRemove = finalRemove;
        this.dateFormat = format;
        this.mongoAuth = true;
        this.mongoUsername = user;
        this.mongoPassword = password;
        this.mongoUserID = userIDField;
        this.mongoItemID = itemIDField;
        this.mongoPreference = preferenceField;
        this.mongoMapCollection = mappingCollection;
        this.reloadLock = new ReentrantLock();
        this.buildModel();
    }

    public void refreshData(String userID, Iterable<List<String>> items, boolean add) throws NoSuchUserException, NoSuchItemException {
        this.checkData(userID, items, add);
        long id = Long.parseLong(this.fromIdToLong(userID, true));
        Iterator i$ = items.iterator();

        while(i$.hasNext()) {
            List item = (List)i$.next();
            item.set(0, this.fromIdToLong((String)item.get(0), false));
        }

        if(this.reloadLock.tryLock()) {
            try {
                if(add) {
                    this.delegate = this.addUserItem(id, items);
                } else {
                    this.delegate = this.removeUserItem(id, items);
                }
            } finally {
                this.reloadLock.unlock();
            }
        }

    }

    public void refresh(Collection<Refreshable> alreadyRefreshed) {
        BasicDBObject query = new BasicDBObject();
        query.put("deleted_at", new BasicDBObject("$gt", this.mongoTimestamp));
        DBCursor cursor = this.collection.find(query);
        Date ts = new Date(0L);

        Map user;
        String userID;
        ArrayList items;
        ArrayList item;
        while(cursor.hasNext()) {
            user = cursor.next().toMap();
            userID = this.getID(user.get(this.mongoUserID), true);
            items = new ArrayList();
            item = new ArrayList();
            item.add(this.getID(user.get(this.mongoItemID), false));
            item.add(Float.toString(this.getPreference(user.get(this.mongoPreference))));
            items.add(item);

            try {
                this.refreshData(userID, items, false);
            } catch (NoSuchUserException var12) {
                log.warn("No such user ID: {}", userID);
            } catch (NoSuchItemException var13) {
                log.warn("No such items: {}", items);
            }

            if(ts.compareTo(this.getDate(user.get("created_at"))) < 0) {
                ts = this.getDate(user.get("created_at"));
            }
        }

        query = new BasicDBObject();
        query.put("created_at", new BasicDBObject("$gt", this.mongoTimestamp));
        cursor = this.collection.find(query);

        while(cursor.hasNext()) {
            user = cursor.next().toMap();
            if(!user.containsKey("deleted_at")) {
                userID = this.getID(user.get(this.mongoUserID), true);
                items = new ArrayList();
                item = new ArrayList();
                item.add(this.getID(user.get(this.mongoItemID), false));
                item.add(Float.toString(this.getPreference(user.get(this.mongoPreference))));
                items.add(item);

                try {
                    this.refreshData(userID, items, true);
                } catch (NoSuchUserException var10) {
                    log.warn("No such user ID: {}", userID);
                } catch (NoSuchItemException var11) {
                    log.warn("No such items: {}", items);
                }

                if(ts.compareTo(this.getDate(user.get("created_at"))) < 0) {
                    ts = this.getDate(user.get("created_at"));
                }
            }
        }

        if(this.mongoTimestamp.compareTo(ts) < 0) {
            this.mongoTimestamp = ts;
        }

    }

    public String fromIdToLong(String id, boolean isUser) {
        return id;
    }

    public String fromLongToId(long id) {
        return new Long(id).toString();
    }

    public boolean isIDInModel(String ID) {
        DBObject objectIdLong = this.collectionMap.findOne(new BasicDBObject("element_id", ID));
        return objectIdLong != null;
    }

    public Date mongoUpdateDate() {
        return this.mongoTimestamp;
    }

    private void buildModel() throws UnknownHostException {
        this.userIsObject = false;
        this.itemIsObject = false;
        this.idCounter = 0L;
        this.preferenceIsString = true;
        Mongo mongoDDBB = new Mongo(this.mongoHost, this.mongoPort);
        DB db = mongoDDBB.getDB(this.mongoDB);
        this.mongoTimestamp = new Date(0L);
        FastByIDMap userIDPrefMap = new FastByIDMap();
        if(!this.mongoAuth || db.authenticate(this.mongoUsername, this.mongoPassword.toCharArray())) {
            this.collection = db.getCollection(this.mongoCollection);
            this.collectionMap = db.getCollection(this.mongoMapCollection);
            BasicDBObject indexObj = new BasicDBObject();
            indexObj.put("element_id", Integer.valueOf(1));
            this.collectionMap.ensureIndex(indexObj);
            indexObj = new BasicDBObject();
            indexObj.put("long_value", Integer.valueOf(1));
            this.collectionMap.ensureIndex(indexObj);
            this.collectionMap.remove(new BasicDBObject());
            DBCursor cursor = this.collection.find();

            while(cursor.hasNext()) {
                Map user = cursor.next().toMap();
                if(!user.containsKey("deleted_at")) {
                    long userID = Long.parseLong(this.fromIdToLong(this.getID(user.get(this.mongoUserID), true), true));
                    long itemID = Long.parseLong(this.fromIdToLong(this.getID(user.get(this.mongoItemID), false), false));
                    float ratingValue = this.getPreference(user.get(this.mongoPreference));
                    Object userPrefs = (Collection)userIDPrefMap.get(userID);
                    if(userPrefs == null) {
                        userPrefs = new ArrayList(2);
                        userIDPrefMap.put(userID, userPrefs);
                    }

                    ((Collection)userPrefs).add(new GenericPreference(userID, itemID, ratingValue));
                    if(user.containsKey("created_at") && this.mongoTimestamp.compareTo(this.getDate(user.get("created_at"))) < 0) {
                        this.mongoTimestamp = this.getDate(user.get("created_at"));
                    }
                }
            }
        }

        this.delegate = new GenericDataModel(GenericDataModel.toDataMap(userIDPrefMap, true));
    }

    private void removeMongoUserItem(String userID, String itemID) {
        String userId = this.fromLongToId(Long.parseLong(userID));
        String itemId = this.fromLongToId(Long.parseLong(itemID));
        if(this.isUserItemInDB(userId, itemId)) {
            this.mongoTimestamp = new Date();
            BasicDBObject query = new BasicDBObject();
            query.put(this.mongoUserID, this.userIsObject?new ObjectId(userId):userId);
            query.put(this.mongoItemID, this.itemIsObject?new ObjectId(itemId):itemId);
            if(this.mongoFinalRemove) {
                log.info(this.collection.remove(query).toString());
            } else {
                BasicDBObject update = new BasicDBObject();
                update.put("$set", new BasicDBObject("deleted_at", this.mongoTimestamp));
                log.info(this.collection.update(query, update).toString());
            }

            log.info("Removing userID: {} itemID: {}", userID, itemId);
        }

    }

    private void addMongoUserItem(String userID, String itemID, String preferenceValue) {
        String userId = this.fromLongToId(Long.parseLong(userID));
        String itemId = this.fromLongToId(Long.parseLong(itemID));
        if(!this.isUserItemInDB(userId, itemId)) {
            this.mongoTimestamp = new Date();
            BasicDBObject user = new BasicDBObject();
            Object userIdObject = this.userIsObject?new ObjectId(userId):userId;
            Object itemIdObject = this.itemIsObject?new ObjectId(itemId):itemId;
            user.put(this.mongoUserID, userIdObject);
            user.put(this.mongoItemID, itemIdObject);
            user.put(this.mongoPreference, this.preferenceIsString?preferenceValue:Double.valueOf(Double.parseDouble(preferenceValue)));
            user.put("created_at", this.mongoTimestamp);
            this.collection.insert(new DBObject[]{user});
            log.info("Adding userID: {} itemID: {} preferenceValue: {}", new Object[]{userID, itemID, preferenceValue});
        }

    }

    private boolean isUserItemInDB(String userID, String itemID) {
        BasicDBObject query = new BasicDBObject();
        Object userId = this.userIsObject?new ObjectId(userID):userID;
        Object itemId = this.itemIsObject?new ObjectId(itemID):itemID;
        query.put(this.mongoUserID, userId);
        query.put(this.mongoItemID, itemId);
        return this.collection.findOne(query) != null;
    }

    private DataModel removeUserItem(long userID, Iterable<List<String>> items) {
        FastByIDMap rawData = ((GenericDataModel)this.delegate).getRawUserData();
        Iterator i$ = items.iterator();

        while(true) {
            PreferenceArray prefs;
            long itemID;
            boolean exists;
            int length;
            do {
                do {
                    if(!i$.hasNext()) {
                        return new GenericDataModel(rawData);
                    }

                    List item = (List)i$.next();
                    prefs = (PreferenceArray)rawData.get(userID);
                    itemID = Long.parseLong((String)item.get(0));
                } while(prefs == null);

                exists = false;
                length = prefs.length();

                for(int newPrefs = 0; newPrefs < length; ++newPrefs) {
                    if(prefs.getItemID(newPrefs) == itemID) {
                        exists = true;
                        break;
                    }
                }
            } while(!exists);

            rawData.remove(userID);
            if(length > 1) {
                GenericUserPreferenceArray var15 = new GenericUserPreferenceArray(length - 1);
                int i = 0;

                for(int j = 0; i < length; ++j) {
                    if(prefs.getItemID(i) == itemID) {
                        --j;
                    } else {
                        var15.set(j, prefs.get(i));
                    }

                    ++i;
                }

                rawData.put(userID, var15);
            }

            log.info("Removing userID: {} itemID: {}", Long.valueOf(userID), Long.valueOf(itemID));
            if(this.mongoManage) {
                this.removeMongoUserItem(Long.toString(userID), Long.toString(itemID));
            }
        }
    }

    private DataModel addUserItem(long userID, Iterable<List<String>> items) {
        FastByIDMap rawData = ((GenericDataModel)this.delegate).getRawUserData();
        Object prefs = (PreferenceArray)rawData.get(userID);
        Iterator i$ = items.iterator();

        while(true) {
            long itemID;
            float preferenceValue;
            boolean exists;
            do {
                if(!i$.hasNext()) {
                    return new GenericDataModel(rawData);
                }

                List item = (List)i$.next();
                itemID = Long.parseLong((String)item.get(0));
                preferenceValue = Float.parseFloat((String)item.get(1));
                exists = false;
                if(prefs != null) {
                    for(int newPrefs = 0; newPrefs < ((PreferenceArray)prefs).length(); ++newPrefs) {
                        if(((PreferenceArray)prefs).getItemID(newPrefs) == itemID) {
                            exists = true;
                            ((PreferenceArray)prefs).setValue(newPrefs, preferenceValue);
                            break;
                        }
                    }
                }
            } while(exists);

            if(prefs == null) {
                prefs = new GenericUserPreferenceArray(1);
            } else {
                GenericUserPreferenceArray var15 = new GenericUserPreferenceArray(((PreferenceArray)prefs).length() + 1);
                int i = 0;

                for(int j = 1; i < ((PreferenceArray)prefs).length(); ++j) {
                    var15.set(j, ((PreferenceArray)prefs).get(i));
                    ++i;
                }

                prefs = var15;
            }

            ((PreferenceArray)prefs).setUserID(0, userID);
            ((PreferenceArray)prefs).setItemID(0, itemID);
            ((PreferenceArray)prefs).setValue(0, preferenceValue);
            log.info("Adding userID: {} itemID: {} preferenceValue: {}", new Object[]{Long.valueOf(userID), Long.valueOf(itemID), Float.valueOf(preferenceValue)});
            rawData.put(userID, prefs);
            if(this.mongoManage) {
                this.addMongoUserItem(Long.toString(userID), Long.toString(itemID), Float.toString(preferenceValue));
            }
        }
    }

    private Date getDate(Object date) {
        if(date.getClass().getName().contains("Date")) {
            return (Date)date;
        } else {
            if(date.getClass().getName().contains("String")) {
                try {
                    DateFormat ioe = this.dateFormat;
                    synchronized(this.dateFormat) {
                        return this.dateFormat.parse(date.toString());
                    }
                } catch (ParseException var5) {
                    log.warn("Error parsing timestamp", var5);
                }
            }

            return new Date(0L);
        }
    }

    private float getPreference(Object value) {
        if(value != null) {
            if(value.getClass().getName().contains("String")) {
                this.preferenceIsString = true;
                return Float.parseFloat(value.toString());
            } else {
                this.preferenceIsString = false;
                return Double.valueOf(value.toString()).floatValue();
            }
        } else {
            return 0.5F;
        }
    }

    private String getID(Object id, boolean isUser) {
        if(id.getClass().getName().contains("ObjectId")) {
            if(isUser) {
                this.userIsObject = true;
            } else {
                this.itemIsObject = true;
            }

            return ((ObjectId)id).toStringMongod();
        } else {
            return id.toString();
        }
    }

    private void checkData(String userID, Iterable<List<String>> items, boolean add) throws NoSuchUserException, NoSuchItemException {
        Preconditions.checkNotNull(userID);
        Preconditions.checkNotNull(items);
        Preconditions.checkArgument(!userID.isEmpty(), "userID is empty");
        Iterator i$ = items.iterator();

        List item;
        while(i$.hasNext()) {
            item = (List)i$.next();
            Preconditions.checkNotNull(item.get(0));
            Preconditions.checkArgument(!((String)item.get(0)).isEmpty(), "item is empty");
        }

        if(this.userIsObject && !ID_PATTERN.matcher(userID).matches()) {
            throw new IllegalArgumentException();
        } else {
            i$ = items.iterator();

            do {
                if(!i$.hasNext()) {
                    if(!add && !this.isIDInModel(userID)) {
                        throw new NoSuchUserException();
                    }

                    i$ = items.iterator();

                    do {
                        if(!i$.hasNext()) {
                            return;
                        }

                        item = (List)i$.next();
                    } while(add || this.isIDInModel((String)item.get(0)));

                    throw new NoSuchItemException();
                }

                item = (List)i$.next();
            } while(!this.itemIsObject || ID_PATTERN.matcher((CharSequence)item.get(0)).matches());

            throw new IllegalArgumentException();
        }
    }

    public void cleanupMappingCollection() {
        this.collectionMap.drop();
    }

    public LongPrimitiveIterator getUserIDs() throws TasteException {
        return this.delegate.getUserIDs();
    }

    public PreferenceArray getPreferencesFromUser(long id) throws TasteException {
        return this.delegate.getPreferencesFromUser(id);
    }

    public FastIDSet getItemIDsFromUser(long userID) throws TasteException {
        return this.delegate.getItemIDsFromUser(userID);
    }

    public LongPrimitiveIterator getItemIDs() throws TasteException {
        return this.delegate.getItemIDs();
    }

    public PreferenceArray getPreferencesForItem(long itemID) throws TasteException {
        return this.delegate.getPreferencesForItem(itemID);
    }

    public Float getPreferenceValue(long userID, long itemID) throws TasteException {
        return this.delegate.getPreferenceValue(userID, itemID);
    }

    public Long getPreferenceTime(long userID, long itemID) throws TasteException {
        return this.delegate.getPreferenceTime(userID, itemID);
    }

    public int getNumItems() throws TasteException {
        return this.delegate.getNumItems();
    }

    public int getNumUsers() throws TasteException {
        return this.delegate.getNumUsers();
    }

    public int getNumUsersWithPreferenceFor(long itemID) throws TasteException {
        return this.delegate.getNumUsersWithPreferenceFor(itemID);
    }

    public int getNumUsersWithPreferenceFor(long itemID1, long itemID2) throws TasteException {
        return this.delegate.getNumUsersWithPreferenceFor(itemID1, itemID2);
    }

    public void setPreference(long userID, long itemID, float value) {
        throw new UnsupportedOperationException();
    }

    public void removePreference(long userID, long itemID) {
        throw new UnsupportedOperationException();
    }

    public boolean hasPreferenceValues() {
        return this.delegate.hasPreferenceValues();
    }

    public float getMaxPreference() {
        return this.delegate.getMaxPreference();
    }

    public float getMinPreference() {
        return this.delegate.getMinPreference();
    }

    public String toString() {
        return "FixedMongoDBDataModel";
    }

    static {
        DEFAULT_DATE_FORMAT = new SimpleDateFormat("EE MMM dd yyyy HH:mm:ss \'GMT\'Z (zzz)", Locale.ENGLISH);
        ID_PATTERN = Pattern.compile("[a-f0-9]{24}");
    }
}
