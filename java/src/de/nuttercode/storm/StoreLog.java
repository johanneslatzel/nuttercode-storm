package de.nuttercode.storm;

import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.time.LocalDateTime;

class StoreLog implements Closeable {

	private final Writer writer;
	private int indentation;
	private boolean isOpen;

	StoreLog(File logFile) throws IOException {
		writer = new FileWriter(logFile, true);
		indentation = 0;
		isOpen = true;
	}

	void log(String message, IndentationAction action) {
		log(message);
		switch (action) {
		case DECREASE:
			decreaseIndentation();
			break;
		case INCREASE:
			increaseIndentation();
			break;
		default:
			break;
		}
	}

	void log(String message) {
		try {
			writer.write(LocalDateTime.now().toString());
			writer.write(": ");
			for (int a = 0; a < indentation; a++)
				writer.write("\t");
			writer.write(message);
			writer.write('\n');
			writer.flush();
		} catch (IOException e) {
			throw new IllegalStateException("can not use log", e);
		}
	}

	void increaseIndentation() {
		indentation++;
	}

	void decreaseIndentation() {
		indentation = indentation == 0 ? 0 : indentation - 1;
	}

	boolean isOpen() {
		return isOpen;
	}

	@Override
	public void close() throws IOException {
		isOpen = false;
		writer.flush();
		writer.close();
	}

}
