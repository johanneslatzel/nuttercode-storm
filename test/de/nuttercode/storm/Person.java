package de.nuttercode.storm;

import java.io.Serializable;

public class Person implements Serializable {

	private static final long serialVersionUID = -5805549273768617073L;

	private String givenName;
	private String lastName;
	private String mail;

	public Person(String vorname, String nachname, String mail) {
		this.givenName = vorname;
		this.lastName = nachname;
		this.mail = mail;
	}

	public String getGivenName() {
		return givenName;
	}

	public String getLastName() {
		return lastName;
	}

	public String getMail() {
		return mail;
	}

	public void setGivenName(String givenName) {
		this.givenName = givenName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}

	public void setMail(String mail) {
		this.mail = mail;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((givenName == null) ? 0 : givenName.hashCode());
		result = prime * result + ((lastName == null) ? 0 : lastName.hashCode());
		result = prime * result + ((mail == null) ? 0 : mail.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Person other = (Person) obj;
		if (givenName == null) {
			if (other.givenName != null)
				return false;
		} else if (!givenName.equals(other.givenName))
			return false;
		if (lastName == null) {
			if (other.lastName != null)
				return false;
		} else if (!lastName.equals(other.lastName))
			return false;
		if (mail == null) {
			if (other.mail != null)
				return false;
		} else if (!mail.equals(other.mail))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Person [givenName=" + givenName + ", lastName=" + lastName + ", mail=" + mail + "]";
	}

}
