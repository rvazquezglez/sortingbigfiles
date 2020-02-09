package org.example.utils

import java.io.*
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter


class LogReader {

    /**
     * Read the files from the input directory, partition them in chunks where the lines are sorted by date,
     * and finally merge them writing to the outputStream.
     *
     * @param inputDirectory the directory to read files
     * @param outputStream where the lines will be printed (can be System.out)
     */
    fun readFilesAndWriteToStream(inputDirectory: String, outputStream: OutputStream) {
        val tempDir = createTempDir()
        val tempPath = tempDir.canonicalPath

        partitionInChunks(inputDirectory, tempPath)

        mergeFiles(tempPath, outputStream)
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
                        var currentFileSizeInBytes = 0
                        val helperList = ArrayList<String>()
                        while (currentFileSizeInBytes < maxFileSizeInBytes && readLine != null) {
                            helperList.add(readLine)
                            currentFileSizeInBytes += readLine.toByteArray(charset).size
                            readLine = bufferedFileReader.readLine()
                        }

                        OutputStreamWriter(
                            FileOutputStream("$outputDirectory/output_chunk_$numFiles.log"),
                            charset
                        ).use { outputChunkFile ->
                            sortLinesByDate(helperList).forEachIndexed { lineNumber, lineContent ->
                                val lineToAppend = if (lineNumber == 0) {
                                    lineContent
                                } else {
                                    "\n$lineContent"
                                }

                                outputChunkFile.append(lineToAppend)
                            }
                        }
                        numFiles++
                    }
                }
            }
    }

    /**
     * Merge all the files from the input directory into one OutputStream, ordering the log entries.
     * Assumption: the files in input directory are already sorted.
     *
     * @param inputDirectory the directory containing the files to be merged
     * @param outputStream where the results will be saved (it could be System.out)
     */
    fun mergeFiles(inputDirectory: String, outputStream: OutputStream) {
        try {
            val mergeBufferedReaders = ArrayList<BufferedReader>()
            val fileEntries = ArrayList<String?>()

            BufferedWriter(OutputStreamWriter(outputStream)).use { bufferedWriter ->
                var someFileStillHasEntries = false
                File(inputDirectory).walk()
                    .filter { it.isFile }
                    .forEach {
                        val logReader = BufferedReader(FileReader(it))
                        // get the first log entry
                        val line = logReader.readLine()
                        if (line != null) {
                            fileEntries.add(line)
                            mergeBufferedReaders.add(logReader)
                            someFileStillHasEntries = true
                        } else {
                            // there are no lines, the reader won't be used anymore
                            logReader.close()
                        }
                    }

                while (someFileStillHasEntries) {
                    val (minIndex, min) = fileEntries.withIndex()
                        .filter { (_, logEntry) -> logEntry != null }
                        .minBy { (_, logEntry) -> getDateFromLine(logEntry!!) }
                        ?: IndexedValue(-1, null)

                    if (minIndex >= 0) {
                        bufferedWriter.appendln(min)

                        fileEntries[minIndex] = mergeBufferedReaders[minIndex].readLine()

                        someFileStillHasEntries = fileEntries.any { it != null }
                    } else {
                        someFileStillHasEntries = false
                    }
                }
            }

            for (bufferedReader in mergeBufferedReaders) {
                bufferedReader.close()
            }

        } catch (ex: java.lang.Exception) {
            ex.printStackTrace()
        }
    }

    companion object {
        fun sortLinesByDate(lines: List<String>): List<String> = lines.sortedBy { getDateFromLine(it) }

        private fun getDateFromLine(line: String): LocalDateTime =
            LocalDateTime.parse(line.substring(0, 19), DateTimeFormatter.ISO_LOCAL_DATE_TIME)
    }

}