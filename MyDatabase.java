/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package MyDatabase;

import com.opencsv.CSVReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author DELL
 */
public class MyDatabase {
    
    static RandomAccessFile file;
    static String[] row = null;
    static long offset;
    static boolean flag=TRUE;
    static Map<String,List<Long>> stateMap=new TreeMap<String,List<Long>>();
    static Map<String,List<Long>> nameMap=new TreeMap<String,List<Long>>();
    static Map<Integer,List<Long>> idMap=new TreeMap<Integer,List<Long>>();
    public static void main(String[] args) throws FileNotFoundException, IOException {
        
        String csvFilename = "us-500.csv";
        File indexfile=new File("id.ndx");
        if(indexfile.exists()){
        ObjectInputStream a=new ObjectInputStream(new FileInputStream("id.ndx"));
            try {
                idMap=(Map<Integer, List<Long>>) a.readObject();
            } catch (ClassNotFoundException ex) {
                Logger.getLogger(MyDatabase.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        else{
        ObjectOutputStream os = new ObjectOutputStream(new FileOutputStream("id.ndx"));
        }
        File namefile=new File("name.ndx");
        if(namefile.exists()){
        ObjectInputStream a=new ObjectInputStream(new FileInputStream("name.ndx"));
            try {
                nameMap=(Map<String, List<Long>>) a.readObject();
            } catch (ClassNotFoundException ex) {
                Logger.getLogger(MyDatabase.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        else{
        ObjectOutputStream os = new ObjectOutputStream(new FileOutputStream("name.ndx"));
        }
        File statefile=new File("state.ndx");
        if(statefile.exists()){
        ObjectInputStream a=new ObjectInputStream(new FileInputStream("state.ndx"));
            try {
                stateMap=(Map<String, List<Long>>) a.readObject();
            } catch (ClassNotFoundException ex) {
                Logger.getLogger(MyDatabase.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        else{
        ObjectOutputStream os = new ObjectOutputStream(new FileOutputStream("state.ndx"));
        }
        
        file=new RandomAccessFile("data.db","rw");
        CSVReader csvReader = new CSVReader(new FileReader(csvFilename),',','\"',1);
        List content = csvReader.readAll();
        
        for (Object object : content) {
            row = (String[]) object;
            int i=Integer.parseInt(row[0]);
            
            offset=file.getFilePointer();
            
            flag=MyDatabase.writeIdMapObject(i,offset);
            if(flag==TRUE)
            {
                MyDatabase.writeNameMapObject(row[1],offset);
                MyDatabase.writeStateMapObject(row[7],offset);
            
                file.writeInt(i);
                file.writeUTF(";"+row[1]+";"+row[7]+";");
            }
            
        }  
        
        System.out.println("Database File written successfully!");
        
        MyDatabase.count();
        
        MyDatabase.selectById(100);
        MyDatabase.selectByName("Olive");
        MyDatabase.selectByState("AK");
        
        MyDatabase.modify("name",304,"rajvi");
        MyDatabase.selectById(304);
        
        MyDatabase.insert(); 
        MyDatabase.selectByName("vama");
        MyDatabase.count();
        MyDatabase.insert();
        
        MyDatabase.delete(304);
        MyDatabase.delete(250);
        
        MyDatabase.count();
        
        MyDatabase.modify("state", 717, "KJ");
        MyDatabase.delete(888); 
        csvReader.close();
    }
    
    static void writeStateMapObject(String state,long offset)
    {
        if(stateMap.containsKey(state))
            {
                List index=stateMap.get(state);
                index.add(offset);
                stateMap.put(state,index);
            }
            else{
                List index=new ArrayList();
                index.add(offset);
                stateMap.put(state,index);
            }
            
            ObjectOutputStream os=null;
            
            try {
                os = new ObjectOutputStream(new FileOutputStream("state.ndx"));
                os.writeObject(stateMap);
                os.close();
            } catch (FileNotFoundException ex) {
                Logger.getLogger(MyDatabase.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                Logger.getLogger(MyDatabase.class.getName()).log(Level.SEVERE, null, ex);
            }
    }
    
    static void writeNameMapObject(String name1,long offset)
    {
        if(nameMap.containsKey(name1))
            {
                List index=nameMap.get(name1);
                index.add(offset);
                nameMap.put(name1,index);
            }
            else{
                List index=new ArrayList();
                index.add(offset);
                nameMap.put(name1,index);
            }
            ObjectOutputStream os=null;
            
            try {
                os = new ObjectOutputStream(new FileOutputStream("name.ndx"));
                os.writeObject(nameMap);
                os.close();
                
            } catch (FileNotFoundException ex) {
                Logger.getLogger(MyDatabase.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                Logger.getLogger(MyDatabase.class.getName()).log(Level.SEVERE, null, ex);
            }
        
    }
    static boolean writeIdMapObject(int i1,long offset)
    {
        if(idMap.containsKey(i1))
            {
                return FALSE;
                
            }
            else{
                List index=new ArrayList();
                index.add(offset);
                idMap.put(i1,index);
                
                ObjectOutputStream os=null;
                
                try {
                    os = new ObjectOutputStream(new FileOutputStream("id.ndx"));
                    os.writeObject(idMap);
                    os.close();
                 } catch (FileNotFoundException ex) {
                    Logger.getLogger(MyDatabase.class.getName()).log(Level.SEVERE, null, ex);
                } catch (IOException ex) {
                    Logger.getLogger(MyDatabase.class.getName()).log(Level.SEVERE, null, ex);
                }
                return TRUE;
            }
    }
    
    static void insert()
    {
        boolean insertflag=TRUE;
        String insertdata="'900','vama','AK'";
        
        String dataarray[]=insertdata.split(",");
        int insertid=Integer.parseInt(dataarray[0].substring(1,dataarray[0].length()-1));
        String insertname=dataarray[1].substring(1,dataarray[1].length()-1);
        String insertstate=dataarray[2].substring(1,dataarray[2].length()-1);
        
        try {
            offset=file.getFilePointer();
        } catch (IOException ex) {
            Logger.getLogger(MyDatabase.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        insertflag=MyDatabase.writeIdMapObject(insertid,offset);
            if(insertflag==TRUE)
            {
            
            try {
                MyDatabase.writeNameMapObject(insertname,offset);
                MyDatabase.writeStateMapObject(insertstate,offset);
                
                file.seek(offset);
                file.writeInt(insertid);
                file.writeUTF(";"+insertname+";"+insertstate+";");
                System.out.println("inserted!");
            } catch (IOException ex) {
                Logger.getLogger(MyDatabase.class.getName()).log(Level.SEVERE, null, ex);
            }
            }
            else
            {
                System.out.println("The id is already in database!");
            }          
    }
    
    static void selectById(int id)
    {
        
            System.out.println("We will find the record where id is "+id);
            
            try {
            ObjectInputStream is2=new ObjectInputStream(new FileInputStream("id.ndx"));
            Map<Integer,List<Long>> map2=(Map<Integer,List<Long>>)is2.readObject();
            
            List index2=map2.get(id);
            if(index2!=null)
            {
                for(int i=0;i<index2.size();i++)
            {
                long idoffset=(long) index2.get(i);
                file.seek(idoffset);
                int getid=file.readInt();
                String namenstate=file.readUTF();
                System.out.print("id;name;state: ");
                System.out.println(getid+namenstate);
            }
                
            }
            else
            {
                System.out.println("The id does not exist in database!");
            }
            
        } catch (IOException ex) {
            Logger.getLogger(MyDatabase.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(MyDatabase.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    static void selectByName(String name)
    {
        
            System.out.println("We will find the record where Name is "+name);
            
            try {
            ObjectInputStream is2=new ObjectInputStream(new FileInputStream("name.ndx"));
            Map<String,List<Long>> map2=(Map<String,List<Long>>)is2.readObject();
            
            List index2=map2.get(name);
            if(index2==null)
            {
            System.out.println("The name does not exist in databse!");
                
            }
            else
            {
                for(int i=0;i<index2.size();i++)
            {
                long idoffset=(long) index2.get(i);
                file.seek(idoffset);
                int getid=file.readInt();
                String namenstate=file.readUTF();
                System.out.print("id;name;state: ");
                System.out.println(getid+namenstate);
            }
            }
        } catch (IOException ex) {
            Logger.getLogger(MyDatabase.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(MyDatabase.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    static void selectByState(String state)
    {
        
            System.out.println("We will find the record where State is "+state);
            
            try {
            ObjectInputStream is2=new ObjectInputStream(new FileInputStream("state.ndx"));
            Map<String,List<Long>> map2=(Map<String,List<Long>>)is2.readObject();
            
            List index2=map2.get(state);
            if(index2!=null)
            {
            for(int i=0;i<index2.size();i++)
            {
                long idoffset=(long) index2.get(i);
                file.seek(idoffset);
                int getid=file.readInt();
                String namenstate=file.readUTF();
                System.out.print("id;name;state: ");
                System.out.println(getid+namenstate);
            }
            }
            else
            {
                System.out.println("The state does not exist in database!");
            }
        } catch (IOException ex) {
            Logger.getLogger(MyDatabase.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(MyDatabase.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    static void modify(String columnname,int id,String newvalue){
        try {
            System.out.println("We will modify the record where id is "+id);
            
            System.out.println("Original record");
            
            ObjectInputStream is1=new ObjectInputStream(new FileInputStream("id.ndx"));
            Map<Integer,List<Long>> map1=(Map<Integer,List<Long>>)is1.readObject();
            
            List index1=map1.get(id);
                if(index1!=null)
                {
                long idoffset=(long) index1.get(0);
                file.seek(idoffset);
                int getid=file.readInt();
                String namenstate=file.readUTF();
                System.out.print("id;name;state: ");
                System.out.println(getid+namenstate);
                
                String[] a=namenstate.split(";");
                String name1=a[1];
                String state1=a[2];
                
                if(columnname.equalsIgnoreCase("name"))   // we will modify name
                {
                    file.seek(idoffset);
                    file.readInt();
                    file.writeUTF(";"+newvalue+";"+state1+";");
                    System.out.println("We will replace name "+name1+" with new name "+newvalue);
                    System.out.println("New modified record");
                    file.seek(idoffset);
                    getid=file.readInt();
                    namenstate=file.readUTF();
                    System.out.print("id;name;state: ");
                    System.out.println(getid+namenstate);
                    
                    modifyNameIndex(name1,idoffset,newvalue);
                    
                }
                
                if(columnname.equalsIgnoreCase("state"))   // we will modify state
                {
                    file.seek(idoffset);
                    file.readInt();
                    file.writeUTF(";"+name1+";"+newvalue+";");
                    System.out.println("We will replace state "+state1+" with new name "+newvalue);
                    System.out.println("New modified record");
                    file.seek(idoffset);
                    getid=file.readInt();
                    namenstate=file.readUTF();
                    System.out.print("id;name;state: ");
                    System.out.println(getid+namenstate);
                    
                    modifyStateIndex(state1,idoffset,newvalue);
                    //System.out.println(file.readChar());
                    List index3=map1.get(id+1);
                    long idoffset1=(long) index3.get(0);
                    file.seek(idoffset1);
                    int getid1=file.readInt();
                    String namenstate1=file.readUTF();
                    System.out.println("next record: ");
                    System.out.print("id;name;state: ");
                    System.out.println(getid1+namenstate1);
                }
                
                }
                else{
                    System.out.println("This id you want to MODIFY does not exist in database!");
                }
        } catch (IOException ex) {
            Logger.getLogger(MyDatabase.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(MyDatabase.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        
    }
    static void modifyNameIndex(String name,long idoffset,String newvalue)
    {
        List index3=nameMap.get(name);
        for(int i=0;i<index3.size();i++)
        {
            if((long)index3.get(i)==idoffset)
            {
                index3.remove(i);
            }
        }
        nameMap.put(name,index3);
        MyDatabase.writeNameMapObject(newvalue, idoffset);
    }
    
    static void modifyStateIndex(String state,long idoffset,String newvalue)
    {
        List index3=stateMap.get(state);
        for(int i=0;i<index3.size();i++)
        {
            if((long)index3.get(i)==idoffset)
            {
                index3.remove(i);
            }
        }
        nameMap.put(state,index3);
        MyDatabase.writeStateMapObject(newvalue, idoffset);
        System.out.println("Map rewritten..yippiee!");
    }
    
    static void count()
    {
        int count=idMap.size();
        System.out.println("Total records: "+count);
    }
    
    static void delete(int id)
    {
        try {
            ObjectInputStream is1=new ObjectInputStream(new FileInputStream("id.ndx"));
            Map<Integer,List<Long>> map1=(Map<Integer,List<Long>>)is1.readObject();
            
            List index1=map1.get(id);
            if(index1!=null)
            {
            long idoffset=(long) index1.get(0);
            file.seek(idoffset);
            int getid=file.readInt();
            String namenstate=file.readUTF();
            
            String[] a=namenstate.split(";");
            String name1=a[1];
            String state1=a[2];
            file.seek(idoffset);
            file.writeInt(0);
            int j=file.readShort();
            //System.out.println(j);
            String s="";
            for(int i=0;i<j;i++)
            {
                s=s+"0";
            }
            file.writeUTF(s);
            
            //file.seek(idoffset);
            //file.readInt();
            //int k=file.readShort();
            //System.out.println(k);
            
            
            List index5=idMap.get(id);
            for(int i=0;i<index5.size();i++)
            {
                if((long)index5.get(i)==idoffset)
                    index5.remove(i);
            }
            idMap.put(id,index5);
            ObjectOutputStream os = new ObjectOutputStream(new FileOutputStream("id.ndx"));
            os.writeObject(idMap);
            os.close();
            List index2=nameMap.get(name1);
            for(int i=0;i<index2.size();i++)
            {
                if((long)index2.get(i)==idoffset)
                    index2.remove(i);
            }
            nameMap.put(name1,index2);
            os = new ObjectOutputStream(new FileOutputStream("name.ndx"));
            os.writeObject(nameMap);
            os.close();
        //MyDatabase.writeNameMapObject(name1, idoffset);
            index2=stateMap.get(state1);
            for(int i=0;i<index2.size();i++)
            {
                if((long)index2.get(i)==idoffset)
                    index2.remove(i);
            }
            stateMap.put(state1, index2);
            os = new ObjectOutputStream(new FileOutputStream("state.ndx"));
            os.writeObject(stateMap);
            os.close();
            }
            else
            {
                System.out.println("This data you want to DELETE does not exist in database!");
            }
        } catch (IOException ex) {
            Logger.getLogger(MyDatabase.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(MyDatabase.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
}
