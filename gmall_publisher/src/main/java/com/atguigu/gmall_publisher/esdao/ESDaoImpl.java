package com.atguigu.gmall_publisher.esdao;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall_publisher.beans.Option;
import com.atguigu.gmall_publisher.beans.SaleDetail;
import com.atguigu.gmall_publisher.beans.Stat;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

@Repository
public class ESDaoImpl implements ESDao{

    @Autowired
    private JestClient jestClient;


    @Override
    public JSONObject getOrderDetail(String date, Integer startpage, Integer size, String keyword) throws IOException {
        SearchResult searchResult = getDataFromEs(date, startpage, size, keyword);
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("total",searchResult.getTotal());
        jsonObject.put("detail",getDetail(searchResult));

        ArrayList<Stat> stats = new ArrayList<>();
        stats.add(new Stat("用户年龄占比",getAgeOptions(searchResult)));
        stats.add(new Stat("用户性别占比",getGenderOptions(searchResult)));
        jsonObject.put("stat",stats);

        return jsonObject;
    }

    //从ES中查询出需要的数据
    private SearchResult getDataFromEs(String date, Integer startpage, Integer size, String keyword) throws IOException {
        String indexName="gmall2_sale_detail_"+ date;

        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("gmall_sale_detail_" + date, keyword);
        TermsBuilder genderAggBuilder = AggregationBuilders.terms("gender_groupCount").field("user_gender").size(10);
        TermsBuilder ageAggBuilder = AggregationBuilders.terms("age_groupCount").field("user_age").size(150);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(matchQueryBuilder)
                .aggregation(genderAggBuilder)
                .aggregation(ageAggBuilder)
                .from((startpage - 1) * size)
                .size(size);

        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex(indexName)
                .addType("_doc")
                .build();

        SearchResult searchResult = jestClient.execute(search);
        return searchResult;
    }

    // 封装 “detail”: [ {SaleDetail},{},{} ]
    public List<SaleDetail> getDetail(SearchResult searchResult){

        List<SaleDetail> result=new ArrayList<SaleDetail>();

        List<SearchResult.Hit<SaleDetail, Void>> hits = searchResult.getHits(SaleDetail.class);

        for (SearchResult.Hit<SaleDetail, Void> hit : hits) {

            SaleDetail saleDetail = hit.source;

            saleDetail.setEs_metadata_id(hit.id);

            result.add(saleDetail);

        }

        return  result;

    }


    // 封装 男女比例的 stat
    public List<Option> getGenderOptions(SearchResult searchResult){

        List<Option> result = new ArrayList<Option>();

        //获取聚合的数据
        MetricAggregation aggregations = searchResult.getAggregations();

        TermsAggregation gender_count = aggregations.getTermsAggregation("gender_count");

        List<TermsAggregation.Entry> buckets = gender_count.getBuckets();

        //存放总人数

        long maleCount=0;
        long femaleCount=0;

        for (TermsAggregation.Entry bucket : buckets) {
            if (bucket.getKey().equals("F")){
                femaleCount = bucket.getCount();
            }else{
                maleCount= bucket.getCount();
            }
        }

        double sumCount=maleCount + femaleCount;

        DecimalFormat decimalFormat = new DecimalFormat("###.00");

        result.add(new Option("男",Double.parseDouble(decimalFormat.format(maleCount / sumCount * 100 ))));
        result.add(new Option("女",Double.parseDouble(decimalFormat.format(100 - (maleCount / sumCount * 100) ))));

        return  result;

    }


    public List<Option> getAgeOptions(SearchResult searchResult){

        List<Option> result = new ArrayList<Option>();

        //获取聚合的数据
        MetricAggregation aggregations = searchResult.getAggregations();

        TermsAggregation gender_count = aggregations.getTermsAggregation("age_count");

        List<TermsAggregation.Entry> buckets = gender_count.getBuckets();

        //存放总人数

        long agelt20Count=0;
        long age20to30Count=0;
        long agegt30Count=0;

        for (TermsAggregation.Entry bucket : buckets) {

            if (Integer.parseInt(bucket.getKey()) < 20){

                agelt20Count += bucket.getCount();

            }else if(Integer.parseInt(bucket.getKey()) >= 30){

                agegt30Count += bucket.getCount();
            }else {
                age20to30Count += bucket.getCount();
            }

        }

        double sumCount= agegt30Count + age20to30Count + agelt20Count;

        DecimalFormat decimalFormat = new DecimalFormat("###.00");

        double percentlt20 = Double.parseDouble(decimalFormat.format(agelt20Count / sumCount * 100));
        double percentgt30 = Double.parseDouble(decimalFormat.format(agegt30Count / sumCount * 100));
        double percent20to30= 100 - percentlt20 - percentgt30;

        result.add(new Option("20岁以下",percentlt20));
        result.add(new Option("20岁到30岁",percent20to30));
        result.add(new Option("30岁及30岁以上",percentgt30));

        return result;

    }
}
