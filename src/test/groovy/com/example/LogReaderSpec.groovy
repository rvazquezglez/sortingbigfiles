package com.example

import org.example.utils.LogReader
import spock.lang.Specification

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class LogReaderSpec extends Specification {
    def outputFileDir = 'src/test/resources/result/output.log'

    def 'Merging two small log files'() {
        given: 'Directory containing two log files'
        def logDir = 'src/test/resources/temp'

        when: 'Reading the files'

        def outputFile = new File(outputFileDir)
        outputFile.createNewFile()
        outputFile.withOutputStream { os ->
            new LogReader().readFilesAndWriteToStream(logDir, os)
        }


        then: 'The output contains the contents of two files sorted by timestamp'
        new File('src/test/resources/expected/output1.log').text == outputFile.text
        outputFile.delete()
    }

    def 'Partition files in chunks'() {
        given: 'Directory containing two log files'
        def logDir = 'src/test/resources/biggerTemp'

        when: 'partition the files in chunks'
        def outputDirString = 'src/test/resources/resultChunks'
        def outputDir = new File(outputDirString)
        outputDir.mkdir()
        new LogReader().partitionInChunks(logDir, outputDirString)

        then: 'The output contains chunk files'
        outputDir.listFiles().size() > 3
        outputDir.deleteDir()
    }

    def 'Sort lines by date'() {
        given: 'file with unsorted entries'
        def fileUnsorted = 'src/test/resources/unsorted/first_unsorted.log'
        when: 'Sorting'
        def sortedLines = new LogReader.Companion().sortLines(new File(fileUnsorted).readLines())
        def fileSortedDir = 'src/test/resources/biggerTemp/first_sorted.log'
        def fileSorted = new File(fileSortedDir)
        fileSorted.withWriterAppend { writer ->
            sortedLines.eachWithIndex { line, index ->
                if (index == 0) {
                    writer.append(line)
                } else {
                    writer.append("\n$line")
                }
            }
        }
        then: 'New file has sorted lines'
        def sortedLinesOutput = new File(fileSortedDir).readLines()
        [sortedLinesOutput.dropRight(1), sortedLinesOutput.drop(1)].transpose().every { String a, String b ->
            getDate(a) <= getDate(b)
        }
        fileSorted.delete()
    }

    private LocalDateTime getDate(String line) {
        return LocalDateTime.parse(line.substring(0, 19), DateTimeFormatter.ISO_LOCAL_DATE_TIME)
    }
}
