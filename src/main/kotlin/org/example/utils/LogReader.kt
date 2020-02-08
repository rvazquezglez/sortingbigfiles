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
                        val outputChunkFile = OutputStreamWriter(
                            FileOutputStream("$outputDirectory/output_chunk_$numFiles.log"),
                            charset
                        )
                        var currentFileSizeInBytes = 0
                        val helperList = ArrayList<String>()
                        while (currentFileSizeInBytes < maxFileSizeInBytes && readLine != null) {
                            helperList.add(readLine)
                            currentFileSizeInBytes += readLine.toByteArray(charset).size
                            readLine = bufferedFileReader.readLine()
                        }

                        outputChunkFile.use {
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
     * Merge all the files from the input directory into one OutputStream, ordering the rows.
     * Assumption: the files in input directory are already sorted.
     *
     * @param inputDirectory the directory containing the files to be merged
     * @param outputStream where the results will be saved (it could be System.out)
     */
    fun mergeFiles(inputDirectory: String, outputStream: OutputStream) {
        try {
            val mergeBufferedReaders = ArrayList<BufferedReader>()
            val fileRows = ArrayList<String?>()
            val bufferedWriter = BufferedWriter(OutputStreamWriter(outputStream))
            bufferedWriter.use {
                var someFileStillHasRows = false
                File(inputDirectory).walk()
                    .filter { it.isFile }
                    .forEach {
                        val fileReader = BufferedReader(FileReader(it))
                        mergeBufferedReaders.add(fileReader)
                        // get the first row
                        val line = fileReader.readLine()
                        if (line != null) {
                            fileRows.add(line)
                            someFileStillHasRows = true
                        }
                    }

                var row: String?
                while (someFileStillHasRows) {
                    var min: LocalDateTime?
                    var minIndex: Int
                    row = fileRows[0]
                    if (row != null) {
                        min = getDateFromLine(row)
                        minIndex = 0
                    } else {
                        min = null
                        minIndex = -1
                    }

                    // check which one is min
                    fileRows.forEachIndexed { i, fileRow ->
                        if (min != null) {
                            if (fileRow != null && getDateFromLine(fileRow) < min) {
                                minIndex = i
                                min = getDateFromLine(fileRow)
                            }
                        } else {
                            if (fileRow != null) {
                                min = getDateFromLine(fileRow)
                                minIndex = i
                            }
                        }
                    }

                    if (minIndex >= 0) {
                        // write to the sorted file
                        bufferedWriter.appendln(fileRows[minIndex])
                        // get another row from the file that had the min
                        fileRows[minIndex] = mergeBufferedReaders[minIndex].readLine()

                        // check if one still has rows
                        someFileStillHasRows = fileRows.any { it != null }
                    } else {
                        someFileStillHasRows = false
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