package de.nuttercode.storm;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer;

import de.nuttercode.util.StringUtil;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class StoreTest {

	private final static long SEED = 42;
	private final static int ITERATIONS = 1_000;

	private static StoreConfiguration STRING_CONF;
	private static StoreConfiguration PERSON1_CONF;
	private static StoreConfiguration PERSON2_CONF;
	private static StoreConfiguration PERSON3_CONF;

	private static Store<String> STRING_STORE;
	private static Store<Person> PERSON1_STORE;
	private static Store<Person> PERSON2_STORE;
	private static Store<Person> PERSON3_STORE;

	@BeforeAll
	static void setup() {
		final Path basePath = Paths.get(System.getProperty("user.dir"));
		STRING_CONF = new StoreConfiguration("stringStore", basePath);
		PERSON1_CONF = new StoreConfiguration("person1Store", basePath);
		PERSON2_CONF = new StoreConfiguration("person2Store", basePath);
		PERSON3_CONF = new StoreConfiguration("person3Store", basePath);
		if (STRING_CONF.getDataFile().exists()) {
			STRING_CONF.getDataFile().delete();
		}
		if (PERSON1_CONF.getDataFile().exists()) {
			PERSON1_CONF.getDataFile().delete();
		}
		if (PERSON2_CONF.getDataFile().exists()) {
			PERSON2_CONF.getDataFile().delete();
		}
		if (PERSON3_CONF.getDataFile().exists()) {
			PERSON3_CONF.getDataFile().delete();
		}
		STRING_CONF.setMinimumDataFileSize(128);
		try {
			STRING_STORE = Store.open(STRING_CONF, StringTransformer.get());
			PERSON1_STORE = Store.open(PERSON1_CONF);
			PERSON2_STORE = Store.open(PERSON2_CONF, new PersonTransformer());
			PERSON3_STORE = Store.open(PERSON3_CONF);
		} catch (IOException e) {
			fail(e);
		}
	}

	@AfterAll
	static void cleanup() {
		try {
			STRING_STORE.close();
			PERSON1_STORE.close();
			PERSON2_STORE.close();
			PERSON3_STORE.close();
		} catch (IOException e) {
			fail(e);
		}
		if (STRING_CONF.getDataFile().exists()) {
			STRING_CONF.getDataFile().delete();
		}
		if (PERSON1_CONF.getDataFile().exists()) {
			PERSON1_CONF.getDataFile().delete();
		}
		if (PERSON2_CONF.getDataFile().exists()) {
			PERSON2_CONF.getDataFile().delete();
		}
		if (PERSON3_CONF.getDataFile().exists()) {
			PERSON3_CONF.getDataFile().delete();
		}
	}

	private void createPerson(Random random, Map<Long, Person> persons, List<Long> ids) {
		try {
			Person person = new Person(StringUtil.randomString(random, 5, 15, null),
					StringUtil.randomString(random, 5, 15, null), StringUtil.randomString(random, 5, 15, null));
			StoreItem<Person> item = PERSON3_STORE.store(person);
			persons.put(item.getId(), person);
			ids.add(item.getId());
			assertTrue(PERSON3_STORE.contains(item.getId()));
			assertTrue(PERSON3_STORE.get(item.getId()).exists());
			assertTrue(persons.containsKey(item.getId()));
			assertTrue(PERSON3_STORE.get(item.getId()).getContent().equals(persons.get(item.getId())));
		} catch (Exception e) {
			fail(e);
		}
	}

	private void deletePerson(Random random, Map<Long, Person> persons, List<Long> ids) {
		try {
			long id = ids.get(random.nextInt(ids.size()));
			assertTrue(PERSON3_STORE.contains(id));
			assertTrue(PERSON3_STORE.get(id).exists());
			assertTrue(persons.containsKey(id));
			assertTrue(PERSON3_STORE.get(id).getContent().equals(persons.get(id)));
			ids.remove(id);
			persons.remove(id);
			PERSON3_STORE.delete(id);
			assertFalse(PERSON3_STORE.contains(id));
			assertFalse(persons.containsKey(id));
		} catch (Exception e) {
			fail(e);
		}
	}

	private void updatePerson(Random random, Map<Long, Person> persons, List<Long> ids) {
		try {
			long id = ids.get(random.nextInt(ids.size()));
			assertTrue(PERSON3_STORE.contains(id));
			assertTrue(PERSON3_STORE.get(id).exists());
			assertTrue(persons.containsKey(id));
			assertTrue(PERSON3_STORE.get(id).getContent().equals(persons.get(id)));
			Person person = PERSON3_STORE.getContent(id);
			person.setGivenName(StringUtil.randomString(random, 5, 15, null));
			person.setLastName(StringUtil.randomString(random, 5, 15, null));
			person.setMail(StringUtil.randomString(random, 5, 15, null));
			persons.put(id, person);
			PERSON3_STORE.update(id, person);
			assertTrue(PERSON3_STORE.contains(id));
			assertTrue(PERSON3_STORE.get(id).exists());
			assertTrue(persons.containsKey(id));
			assertTrue(PERSON3_STORE.get(id).getContent().equals(persons.get(id)));
		} catch (IOException e) {
			fail(e);
		}
	}

	private void reopen(Map<Long, Person> persons, List<Long> ids) {
		try {
			PERSON3_STORE.close();
			PERSON3_STORE = Store.open(PERSON3_CONF);
			for (long id : ids) {
				assertTrue(PERSON3_STORE.contains(id));
				assertTrue(PERSON3_STORE.get(id) != null);
				assertTrue(PERSON3_STORE.get(id).exists());
				assertTrue(persons.containsKey(id));
				assertTrue(PERSON3_STORE.get(id).getContent().equals(persons.get(id)));
			}
			for (long id : PERSON3_STORE.getIds()) {
				assertTrue(ids.contains(id));
			}
		} catch (IOException e) {
			fail(e);
		}
	}

	@Test
	@Order(5)
	void testPersonStoreRandom() {
		Random random = new Random(SEED);
		RandomOption[] options = RandomOption.values();
		Map<Long, Person> persons = new HashMap<>();
		List<Long> ids = new ArrayList<>();
		for (int a = 0; a < ITERATIONS; a++) {
			switch (options[random.nextInt(options.length)]) {
			case CREATE:
				createPerson(random, persons, ids);
				break;
			case DELETE:
				if (!ids.isEmpty())
					deletePerson(random, persons, ids);
				else
					a--;
				break;
			case REOPEN:
				reopen(persons, ids);
				break;
			case UPDATE:
				if (!ids.isEmpty())
					updatePerson(random, persons, ids);
				else
					a--;
				break;
			default:
				fail(new IllegalStateException("unrecognized option"));
			}
		}
	}

	@Test
	@Order(1)
	void testStringStore() throws IOException {

		final String message1 = "Hallo Welt!";
		final String message2 = "Auf Wiedersehen " + System.currentTimeMillis() + "!";
		long previosID;
		int size = STRING_STORE.size();
		StoreItem<String> item1, item2;

		item1 = STRING_STORE.store(message1);
		assertTrue(item1 != null);
		assertTrue(item1.exists());
		assertTrue(item1.getContent().equals(message1));
		assertTrue(STRING_STORE.size() == size + 1);
		item2 = STRING_STORE.store(message2);
		assertTrue(item1 != null);
		assertTrue(item2.exists());
		assertTrue(item2.getId() == item1.getId() + 1);
		assertTrue(item2.getContent().equals(message2));
		assertTrue(STRING_STORE.size() == size + 2);
		item2.delete();
		assertTrue(STRING_STORE.size() == size + 1);
		assertTrue(!item2.exists());
		previosID = item1.getId();
		item1.update(message2);
		assertTrue(item1 != null);
		assertTrue(item1.exists());
		assertTrue(item1.getContent().equals(message2));
		assertTrue(item1.getId() == previosID);
		assertTrue(STRING_STORE.size() == size + 1);

	}

	@Test
	@Order(2)
	void testPersonStore() throws IOException {

		Person max = new Person("max", "mustermann", "max.mustermann@domain.com");
		Person maxine = new Person("maxine", "musterfrau", "maxine.musterfrau@domain.typo.com");
		StoreItem<Person> itemMax, itemMaxine;
		if (!PERSON1_STORE.contains(500))
			itemMax = PERSON1_STORE.store(max);
		else
			itemMax = PERSON1_STORE.get(500);
		assertTrue(itemMax != null);
		assertTrue(itemMax.exists());
		assertTrue(itemMax.getContent().equals(max));
		if (!PERSON1_STORE.contains(501))
			itemMaxine = PERSON1_STORE.store(maxine);
		else
			itemMaxine = PERSON1_STORE.get(501);
		assertTrue(itemMaxine != null);
		assertTrue(itemMaxine.exists());
		assertTrue(itemMaxine.getContent().equals(maxine));
		maxine.setMail("maxine.musterfrau@domain.com");
		itemMaxine.update(maxine);
		assertTrue(itemMaxine != null);
		assertTrue(itemMaxine.exists());
		assertTrue(itemMaxine.getContent().equals(maxine));

	}

	@Test
	@Order(3)
	void testPersonStoreSerializable() throws IOException {

		Person max = new Person("max", "mustermann", "max.mustermann@domain.com");
		Person maxine = new Person("maxine", "musterfrau", "maxine.musterfrau@domain.typo.com");
		StoreItem<Person> itemMax, itemMaxine;
		if (!PERSON2_STORE.contains(500))
			itemMax = PERSON2_STORE.store(max);
		else
			itemMax = PERSON2_STORE.get(500);
		assertTrue(itemMax != null);
		assertTrue(itemMax.exists());
		assertTrue(itemMax.getContent().equals(max));
		if (!PERSON2_STORE.contains(501))
			itemMaxine = PERSON2_STORE.store(maxine);
		else
			itemMaxine = PERSON2_STORE.get(501);
		assertTrue(itemMaxine != null);
		assertTrue(itemMaxine.exists());
		assertTrue(itemMaxine.getContent().equals(maxine));
		maxine.setMail("maxine.musterfrau@domain.com");
		itemMaxine.update(maxine);
		assertTrue(itemMaxine != null);
		assertTrue(itemMaxine.exists());
		assertTrue(itemMaxine.getContent().equals(maxine));

	}

	@Test
	@Order(4)
	void comparePersonStore() throws IOException {
		assertTrue(PERSON1_STORE.contains(500));
		assertTrue(PERSON1_STORE.contains(501));
		assertTrue(PERSON2_STORE.contains(500));
		assertTrue(PERSON2_STORE.contains(501));
		assertTrue(PERSON1_STORE.get(500).getContent().equals(PERSON2_STORE.get(500).getContent()));
		assertTrue(PERSON1_STORE.get(501).getContent().equals(PERSON2_STORE.get(501).getContent()));
		assertTrue(PERSON1_STORE.size() == 2);
		assertTrue(PERSON2_STORE.size() == 2);
	}

}
