package com.splicemachine;

import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.util.DefaultClassResolver;
import com.esotericsoftware.kryo.util.IntMap;
import com.esotericsoftware.kryo.util.ObjectMap;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.derby.hbase.SpliceDriver;

import static com.esotericsoftware.kryo.util.Util.className;
import static com.esotericsoftware.minlog.Log.TRACE;
import static com.esotericsoftware.minlog.Log.trace;

/**
 * @author Scott Fines
 *         Date: 1/5/16
 */
public class DerbyClassResolver extends DefaultClassResolver{

    private ClassFactory classFactory;

    public DerbyClassResolver(){
    }

    @Override
    protected Registration readName(Input input){
        int nameId = input.readInt(true);
        if (nameIdToClass == null) nameIdToClass =new IntMap<>();
        Class type = nameIdToClass.get(nameId);
        if (type == null) {
            // Only read the class name the first time encountered in object graph.
            String className = input.readString();
            if (nameToClass != null) type = nameToClass.get(className);
            if (type == null) {
                type=loadClass(className,true);
                if (nameToClass == null) nameToClass = new ObjectMap<>();
                nameToClass.put(className, type);
            }
            nameIdToClass.put(nameId, type);
            if (TRACE) trace("kryo", "Read class name: " + className);
        } else {
            if (TRACE) trace("kryo", "Read class name reference " + nameId + ": " + className(type));
        }
        return kryo.getRegistration(type);
    }

    private Class loadClass(String className,boolean allowRetry){
        try {
            return Class.forName(className, false, kryo.getClassLoader());
        } catch (ClassNotFoundException ex) {
            if(!allowRetry)
                throw new KryoException("Unable to find class: " + className, ex);
            /*
             * This can happen sometimes with UDTs, which are listed
             * as part of derby's special class path, rather than the
             * default classpath which we go for initially. In this
             * case, we try to resolve it using the classFactory, then try again
             */
            Class c = loadFromClassFactory(className);
            if(c==null)
                throw new KryoException("Unable to find class: " + className, ex);
            else return c;
        }
    }

    private Class loadFromClassFactory(String className){
        if(classFactory==null){
            EmbedConnection dbConn = (EmbedConnection) SpliceDriver.driver().getInternalConnection();
            LanguageConnectionContext lcc = dbConn.getLanguageConnection();
            this.classFactory = lcc.getLanguageConnectionFactory().getClassFactory();
        }
        try{
            return classFactory.loadApplicationClass(className);
        }catch(ClassNotFoundException e){
            return null;
        }
    }
}
