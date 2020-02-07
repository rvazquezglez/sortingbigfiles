package org.example.utils

import java.io.*
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter


class LogReader {
    fun readFilesAndWriteToStream(directory: String, outputStream: OutputStream) {
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

    fun partitionInChunks(inputDirectory: String, outputDirectory: String) {
        val charset = Charsets.UTF_8
        val maxFileSizeInBytes = 3000

        var numFiles = 0
        File(inputDirectory).walk()
            .filter { it.isFile }
            .forEach {
                val bufferedFileReader = BufferedReader(FileReader(it))
                bufferedFileReader.use {
                    var readLine: String? = bufferedFileReader.readLine()
                    while (readLine != null) {
                        val outputChunkFile = OutputStreamWriter(
                            FileOutputStream("$outputDirectory/output_chunk_$numFiles.log"),
                            charset
                        )
                        var currentFileSizeInBytes = 0
                        outputChunkFile.use {
                            while (currentFileSizeInBytes < maxFileSizeInBytes && readLine != null) {
                                val lineToAppend: String = if (currentFileSizeInBytes == 0) {
                                    readLine!!
                                } else {
                                    "\n$readLine"
                                }
                                outputChunkFile.append(lineToAppend)
                                currentFileSizeInBytes += lineToAppend.toByteArray(charset).size

                                readLine = bufferedFileReader.readLine()
                            }
                        }
                        numFiles++
                    }
                }
            }
    }

    companion object {
        fun sortLines(lines: List<String>): List<String> = lines.sortedBy { getDateFromLine(it) }


        private fun getDateFromLine(line: String): LocalDateTime =
            LocalDateTime.parse(line.substring(0, 19), DateTimeFormatter.ISO_LOCAL_DATE_TIME)
    }

}