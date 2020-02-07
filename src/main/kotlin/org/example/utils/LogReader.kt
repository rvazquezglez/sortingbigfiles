package org.example.utils

import java.io.File
import java.io.OutputStream
import java.io.PrintWriter

class LogReader {
    private fun readFilesAndWriteToStream(directory: String, outputStream: OutputStream) {
        val printWriter = PrintWriter(outputStream)
        File(directory).walk()
            .filter { it.isFile }
            .forEach {
                printWriter.append(it.readText(Charsets.UTF_8))
                printWriter.println()
            }
        printWriter.flush()
    }
}