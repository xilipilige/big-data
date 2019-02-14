import com.agioe.big.data.redis.example.RedisApplication;
import com.agioe.big.data.redis.example.model.Event;
import com.agioe.big.data.redis.example.model.Person;
import com.agioe.big.data.redis.example.repository.EventRepository;
import com.agioe.big.data.redis.example.repository.PersonRepository;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.LinkedList;
import java.util.List;


@RunWith(SpringRunner.class)
@SpringBootTest(classes={RedisApplication.class})
public class ApplicationTest {

    @Autowired
    private PersonRepository repo;

    @Autowired
    private EventRepository eventRepository;

    @Test
    public void basicCrudOperations(){
        Person person = new Person("贾", "大贤");

        repo.save(person);

        repo.findOne(person.getId());

        repo.count();

//        repo.delete(rand);
    }



    @Test
    public void eventTest(){
        List<Event> list = new LinkedList<>();
        for(int i=1;i<10000;i++){
            Event event = new Event();
            event.setDeviceCode("dev"+i);
            event.setNature("1");
            event.setClassify("EVT01");
            event.setSubClassify("EVT01-SUB02");
            event.setState("2");
            event.setLevel("3");
            event.setTime(String.valueOf(System.currentTimeMillis()));
            list.add(event);
        }
        long start =System.currentTimeMillis();
        eventRepository.save(list);
        System.out.println("耗时"+(System.currentTimeMillis()-start));
    }

    @Test
    public void findEvent(){
        long start =System.currentTimeMillis();
        Iterable<Event> all = eventRepository.findAll();
        System.out.println("耗时"+(System.currentTimeMillis()-start));
    }
}
