import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

public class App {
    public static void main(String[] args) throws Exception {
        Class.forName("org.postgresql.Driver");
        searcher s = new searcher();
        s.start();
        s.markHTML();
    }
}

class searcher {
    private Connection conn;
    private String[] words;
    private ArrayList<Integer> wrowid = new ArrayList<Integer>(),
        rows = new ArrayList<Integer>();
    private Map<Integer, Double> norm1 = new LinkedHashMap<Integer, Double>(),
        norm2 = new LinkedHashMap<Integer, Double>(),
        m3 = new LinkedHashMap<Integer, Double>();
    searcher()
    {
        wrowid.clear();
        rows.clear();
        norm1.clear();
        norm2.clear();
        m3.clear();
        try
        {
            conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/isdatabase", "postgres", "0000");
            conn.setAutoCommit(false);
            System.out.println("nice");
        }
        catch (SQLException e) { System.out.println(e.getMessage()); }
    }
    void start()
    {
        Scanner sc = new Scanner(System.in, "Cp866");
        System.out.println("Поиск:");
        String str = sc.nextLine();
        sc.close();
        if (!getQuery(str) || !getWordsIds())
            return;
        getMs();
    }

    private boolean getQuery(String str)
    {
        words = str.toLowerCase().split(" ");
        if (words.length < 2)
        {
            System.out.println("В запросе должно быть как минимум 2 слова, разделенных пробелом.");
            return false;
        }
        return true;
    }

    private boolean getWordsIds()
    {
        for (int i = 0; i < 2; i++)
        {
            String q = "SELECT rowid FROM wordlist WHERE word = '" + words[i] + "'";
            try
            {
                Statement stmt = conn.createStatement();
                ResultSet res = stmt.executeQuery(q);
                res.next();
                wrowid.add(res.getInt("rowid"));
            }
            catch (SQLException e)
            {
                System.out.println("\nСлово " + words[i] + " не найдено.");
                return false;
            }
        }
        return true;
    }

    private void getMatchRows()
    {
        ArrayList<String> name = new ArrayList<String>();
        ArrayList<String> join = new ArrayList<String>();
        ArrayList<String> cond = new ArrayList<String>();
        for (int i = 0; i < wrowid.size(); i++)
        {
            if (i == 0)
            {
                name.add("w0.fk_urlid");
                name.add(", w0.location w0_loc");
                cond.add("WHERE w0.fk_wordid = " + wrowid.get(i));
            }
            else
            {
                name.add(", w" + i + ".location w" + i + "_loc");
                join.add("INNER JOIN wordLocation w" + i + " ON w0.fk_urlid = w" + i + ".fk_urlid");
                cond.add(" AND w" + i + ".fk_wordid = " + wrowid.get(i));
            }
        }
        String q = "SELECT ";
        for (String str : name)
            q += "\n" + str;
        q += "\nFROM wordLocation w0 ";
        for (String str : join)
            q += "\n" + str;
        for (String str : cond)
            q += "\n" + str;
        
        try
        {
            Statement stmt = conn.createStatement();
            ResultSet res = stmt.executeQuery(q);
            while (res.next())
            {
                rows.add(res.getInt("fk_urlid"));
                for (int i = 0; i < wrowid.size(); i++)
                    rows.add(res.getInt("w" + i + "_loc"));
            }
        }
        catch (SQLException e) { System.out.println(e.getMessage() + 1); }
    }

    private Map<Integer, Double> normalizeScores(Map<Integer, Double> scores, boolean small)
    {
        double i = -1, min = 0, max = 0;
        Map<Integer, Double> norm = new HashMap<Integer, Double>();
        for (Map.Entry<Integer, Double> sc : scores.entrySet())
        {
            i++;
            if (i == 0)
            {
                min = sc.getValue();
                max = sc.getValue();
                continue;
            }
            min = Math.min(min, sc.getValue());
            max = Math.max(max, sc.getValue());
        }
        for (Map.Entry<Integer, Double> sc : scores.entrySet())
        {
            if (small)
                norm.put(sc.getKey(), (double)min / Math.max(0.00001, sc.getValue()));
            else
                norm.put(sc.getKey(), (double)sc.getValue() / (double)max);
        }
        return norm;
    }
    private Map<Integer, Double> sort(Map<Integer, Double> a)
    {
        List<Map.Entry<Integer, Double>> sort = new ArrayList<>(a.entrySet());
        sort.sort(Map.Entry.comparingByValue());
        Collections.reverse(sort);
        a = new LinkedHashMap<Integer, Double>();
        for (Map.Entry<Integer, Double> e : sort)
            a.put(e.getKey(), e.getValue());
        return a;
    }

    private void frequencyScore()
    {
        Map<Integer, Double> scores = new HashMap<Integer, Double>();
        ArrayList<Double> b = new ArrayList<Double>();
        for (int i = 1; i <= 100; i++)
        {
            double a = 1;
            b.clear();
            for (int j = 0; j < wrowid.size(); j++)
                b.add(0.0);
            for (int j = 0; j < rows.size(); j += wrowid.size() + 1)
            {
                if (i != rows.get(j))
                    continue;
                for (int k = 1; k <= wrowid.size(); k++)
                    b.set(k - 1, b.get(k - 1) + 1);
            }
            for (int j = 0; j < wrowid.size(); j++)
                a *= b.get(j);
            scores.put(i, a);
        }
        norm2.putAll(sort(normalizeScores(scores, false)));
    }
    
    private void PageRank(int N)
    {
        String q = "DROP TABLE IF EXISTS pagerank;"
            + "CREATE TABLE IF NOT EXISTS pagerank("
            + "rowid SERIAL PRIMARY KEY, "
            + "urlid INTEGER, "
            + "score REAL);"
            + "INSERT INTO pagerank (urlid, score) SELECT rowid, 1.0 FROM urllist";
        try
        {
            Statement stmt = conn.createStatement();
            stmt.execute(q);
            conn.commit();
        }
        catch (SQLException e) { System.out.println(e.getMessage() + 3); }

        Map<Integer, Integer> c = new HashMap<Integer, Integer>();
        q = "SELECT fk_fromurl_id, COUNT(fk_tourl_id) FROM linkbetweenurl GROUP BY fk_fromurl_id ORDER BY fk_fromurl_id";
        try
        {
            Statement stmt = conn.createStatement();
            ResultSet res = stmt.executeQuery(q);
            while (res.next())
                c.put(res.getInt("fk_fromurl_id"), res.getInt("count"));
        }
        catch (SQLException e) { System.out.println(e.getMessage() + 4); }

        ArrayList<Double> s = new ArrayList<Double>();
        for (int i = 0; i < N; i++)
        {
            System.out.println("Итерация " + (i + 1));
            s.clear();
            try
            {
                q = "SELECT score FROM pagerank";
                Statement stmt = conn.createStatement();
                ResultSet res = stmt.executeQuery(q);
                while (res.next())
                    s.add(res.getDouble("score"));
            }
            catch (SQLException e) { System.out.println(e.getMessage() + 5); }

            for (int j = 1; j <= 100; j++)
            {
                double pr = 0;
                try
                {
                    q = "SELECT fk_fromurl_id FROM linkbetweenurl WHERE fk_tourl_id = " + j + " AND fk_fromurl_id != fk_tourl_id";
                    Statement stmt = conn.createStatement();
                    ResultSet res = stmt.executeQuery(q);
                    while (res.next())
                        pr += s.get(res.getInt("fk_fromurl_id")) / c.get(res.getInt("fk_fromurl_id"));
                    
                    q = "UPDATE pagerank SET score = " + (0.15 + 0.85 * pr) + " WHERE urlid = " + j;
                    stmt.execute(q);
                }
                catch (SQLException e) { System.out.println(e.getMessage() + 6); }
            }
            try
            {
                conn.commit();
            }
            catch (SQLException e) {}
        }
    }
    private void pagerankScore()
    {
        PageRank(5);
        Map<Integer, Double> scores = new HashMap<Integer, Double>();
        try
        {
            String q = "SELECT urlid, score FROM pagerank WHERE urlid <= 100";
            Statement stmt = conn.createStatement();
            ResultSet res = stmt.executeQuery(q);
            while (res.next())
                scores.put(res.getInt("urlid"), res.getDouble("score"));
        }
        catch (SQLException e) { System.out.println(e.getMessage() + 7); }
        norm1.putAll(sort(normalizeScores(scores, false)));
    }

    private void findm3()
    {
        Map<Integer, Double> a = new HashMap<Integer, Double>();
        for (int i = 1; i <= norm1.size(); i++)
            a.put(i, (norm1.get(i) + norm2.get(i)) / 2);
        m3.putAll(sort(a));
    }

    private String getUrlName(int id)
    {
        String q = "SELECT url FROM urllist WHERE rowid = '" + id + "'";
        try
        {
            Statement stmt = conn.createStatement();
            ResultSet res = stmt.executeQuery(q);
            res.next();
            return res.getString("url");
        }
        catch (SQLException e)
        {
            System.out.println(e.getMessage() + 2);
            return null;
        }
    }
    private void printMs(Map<Integer, Double> M, int N)
    {
        int i = 0;
        for (Map.Entry<Integer, Double> m : M.entrySet())
        {
            if (i++ >= N)
                break;
            System.out.println(m.getKey() + " " + getUrlName(m.getKey()) + " " + m.getValue());
        }
    }

    private void getMs()
    {
        getMatchRows();
        frequencyScore();
        pagerankScore();
        findm3();

        int N = 10;
        System.out.println("M1:");
        printMs(norm1, N);
        System.out.println("M2:");
        printMs(norm2, N);
        System.out.println("M3:");
        printMs(m3, N);
    }

    private void createHTML(String url, String filename)
    {
        try
        {
            Document doc = Jsoup.connect(url).userAgent("Chrome/81.0.4044.138").get();
            String text = doc.text().toLowerCase();
            int i;
            for (String str : words)
            {
                String[] s = text.split(str);
                text = "<html><head><title>doc</title></head><body><p>";
                for (i = 0; i < s.length - 1; i++)
                {
                    text += s[i];
                    if (s[i].matches(".*[\\p{L}]") || s[i + 1].matches("[\\p{L}].*"))
                        text += str;
                    else
                        text += "<text style=\"background-color:yellow;\">" + str + "</text>";
                }
                text += s[i] + "</p></body></html>";
            }

            PrintWriter f = new PrintWriter(new FileWriter(filename, StandardCharsets.UTF_8));
            f.write(text);
            f.close();
        }
        catch (IOException e) { System.out.println(e.getMessage() + 8); }
    }
    void markHTML()
    {
        int i = 0;
        for (Map.Entry<Integer, Double> m : m3.entrySet())
        {
            if (i++ >= 3)
                break;
            System.out.println("Документ " + i);
            createHTML(getUrlName(m.getKey()), "doc" + i + ".html");
        }
        System.out.println("nice 2.0");
    }
}