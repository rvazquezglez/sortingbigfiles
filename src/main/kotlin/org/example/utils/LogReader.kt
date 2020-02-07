package org.example.utils

import java.io.*


class LogReader {
    private fun readFilesAndWriteToStream(directory: String, outputStream: OutputStream) {
        val printWriter = PrintWriter(outputStream)
        printWriter.use {
            File(directory).walk()
                .filter { it.isFile }
                .forEach {
                    printWriter.append(it.readText(Charsets.UTF_8))
                    printWriter.println()
                }
        }
    }

    private fun partitionInChunks(inputDirectory: String, outputDirectory: String) {
        val charset = Charsets.UTF_8
        val maxFileSizeInBytes = 300

        File(inputDirectory).walk()
            .filter { it.isFile }
            .forEach {
                val bufferedFileReader = BufferedReader(FileReader(it))
                bufferedFileReader.use {
                    var readLine: String? = bufferedFileReader.readLine()
                    var numFiles = 0
                    while (readLine != null) {
                        val outputChunkFile = OutputStreamWriter(
                            FileOutputStream("$outputDirectory/output_chunk_$numFiles.log"),
                            charset
                        )
                        var currentFileSizeInBytes = 0
                        outputChunkFile.use {
                            while (currentFileSizeInBytes < maxFileSizeInBytes && readLine != null) {
                                outputChunkFile.appendln(readLine)
                                readLine = bufferedFileReader.readLine()
                                if (readLine != null) {
                                    currentFileSizeInBytes += readLine!!.toByteArray(charset).size
                                }
                            }
                        }
                        numFiles++
                    }
                }
            }
    }
}