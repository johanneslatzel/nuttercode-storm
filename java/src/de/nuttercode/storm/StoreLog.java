package de.nuttercode.storm;

import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.time.LocalDateTime;

class StoreLog implements Closeable {

	private final Writer writer;

	StoreLog(File logFile) throws IOException {
		writer = new FileWriter(logFile, true);
	}

	void log(String message) {
		try {
			writer.write(LocalDateTime.now() + ": " + message + "\n");
			writer.flush();
		} catch (IOException e) {
			throw new IllegalStateException("can not use log", e);
		}
	}

	@Override
	public void close() throws IOException {
		writer.flush();
		writer.close();
	}

}
