import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Created by martini on 2016-03-22.
 */
public class ByYearServlet extends HttpServlet {
    private DataBase database;
    private Table movieTable;


    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

    }


    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        PrintWriter reply = response.getWriter();
        String param = request.getQueryString();


        System.out.println("ByYear!");
        int query = Integer.parseInt(param);
        System.out.println("Querying mooovies!");
        String result = "movies from: "+param;


        HashMap<String, String> nameMap = new HashMap<String, String>();
        nameMap.put("#yr", "year");

        HashMap<String, Object> valueMap = new HashMap<String, Object>();
        valueMap.put(":yyyy", query);

        QuerySpec querySpec = new QuerySpec()
                .withKeyConditionExpression("#yr = :yyyy")
                .withNameMap(new NameMap().with("#yr", "year"))
                .withValueMap(valueMap);


        ItemCollection<QueryOutcome> items;
        Iterator<Item> iterator;
        Item item;
        try {
            items = movieTable.query(querySpec);

            iterator = items.iterator();
            while (iterator.hasNext()) {
                item = iterator.next();
                result +="\n"+ item.getNumber("year") + ": "
                        + item.getString("title");
            }

        }catch(Exception e){
            reply.write("Unable to query movies from " + param + " \n" + e.getMessage());
        }



        reply.write(result);





    }


    public void init(){
        database = DataBase.Factory.GetDataBase();
        movieTable = database.getMovieTable();
    }
}
