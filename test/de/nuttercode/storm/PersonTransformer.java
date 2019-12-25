package de.nuttercode.storm;


import de.nuttercode.storm.ObjectTransformer;
import de.nuttercode.util.buffer.ReadableBuffer;
import de.nuttercode.util.buffer.WritableBuffer;

public class PersonTransformer implements ObjectTransformer<Person> {

	@Override
	public void putInto(Person value, WritableBuffer writableBuffer) {
		writableBuffer.putString(value.getGivenName());
		writableBuffer.putString(value.getLastName());
		writableBuffer.putString(value.getMail());
	}

	@Override
	public Person getFrom(ReadableBuffer readableBuffer) {
		return new Person(readableBuffer.getString(), readableBuffer.getString(), readableBuffer.getString());
	}

}
