package com.agioe.big.data.hbase.es;

import com.agioe.big.data.hbase.es.es.Employee;
import com.agioe.big.data.hbase.es.es.EmployeeRepository;
import com.google.gson.Gson;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.query.Criteria;
import org.springframework.data.elasticsearch.core.query.CriteriaQuery;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;


/**
 * @author yshen
 * @since 19-3-1
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
public class ESTest {

    @Autowired
    private EmployeeRepository employeeRepository;

    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;

    /**
     * 添加
     *
     * @return
     */
    @Test
    public void add() {
        Employee employee = new Employee();
        employee.setId(UUID.randomUUID().toString());
        employee.setFirstName("xuxu");
        employee.setLastName("sun");
        employee.setAge(21);
        employee.setAbout("i am in peking");
        employeeRepository.save(employee);
        System.err.println("add a obj");
    }

    /**
     * 删除
     *
     * @return
     */
    @Test
    public void delete() {
        Employee employee = employeeRepository.queryEmployeeById("1");
        employeeRepository.delete(employee);
    }

    /**
     * 局部更新
     *
     * @return
     */
    @Test
    public void update() {
        Employee employee = employeeRepository.queryEmployeeById("1");
        employee.setFirstName("哈哈");
        employeeRepository.save(employee);
        System.err.println("update a obj");
    }

    /**
     * 查询
     *
     * @return
     */
    @Test
    public void query() {
        Employee employee = employeeRepository.queryEmployeeById("1");
        System.err.println(new Gson().toJson(employee));
    }


    /**
     * 查询
     *
     * @return
     */
    @Test
    public void queryByField() {
        List<Employee> employee = employeeRepository.queryEmployeeByFirstNameOrderByAgeAsc("xuxu");
        System.err.println(new Gson().toJson(employee));
    }


    /**
     * 分页查询
     *
     * @return
     */
    @Test
    public void queryByPage() {
        Pageable pageable = PageRequest.of(0, 3);
        Page<Employee> employees = employeeRepository.getPageByFirstName("xuxu", pageable);
        System.err.println(new Gson().toJson(employees));
    }


    /**
     * 查询所有
     *
     * @return
     */
    @Test
    public void queryAll() {
        Iterable<Employee> iterable = employeeRepository.findAll();

        iterable.forEach(s -> {
            System.err.println(new Gson().toJson(s));

        });

    }

    /**
     * 查询个数
     *
     * @return
     */
    @Test
    public void queryCount() {
        long count = employeeRepository.count();
        System.out.println(count);
    }

    /**
     * 多条件组合查询
     *
     * @see: https://blog.csdn.net/u014016928/article/details/41723485
     */
    @Test
    public void queryCriteria() {
        CriteriaQuery criteriaQuery = new CriteriaQuery(
                new Criteria()
                        .and(new Criteria("firstName").is("xuxu"))
                        .and(new Criteria("lastName").is("zh"))
                        .and(new Criteria("about").is("i am in peking"))
        );
        List<Employee> employee = elasticsearchTemplate.queryForList(criteriaQuery, Employee.class);
        System.err.println(new Gson().toJson(employee));
    }


    @Test
    /**
     * In 查询
     * @see :http://www.cnblogs.com/wenbronk/p/6432990.html
     */
    public void queryIn() {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery("lastName", "shen"))
                .should(QueryBuilders.termQuery("lastName", "sun"));
        SearchRequestBuilder searchRequestBuilder =
                elasticsearchTemplate.getClient().prepareSearch("company").setTypes("employee");

        SearchResponse searchResponse = searchRequestBuilder.setQuery(boolQueryBuilder).execute().actionGet();


        for (SearchHit hit : searchResponse.getHits()) {
            Iterator<Map.Entry<String, Object>> iterator = hit.getSource().entrySet().iterator();
            while(iterator.hasNext()) {
                Map.Entry<String, Object> next = iterator.next();
                System.out.println(next.getKey() + ": " + next.getValue());
                if(searchResponse.getHits().hits().length == 0) {
                    break;
                }
            }
        }
    }

}
