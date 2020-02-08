package com.example

import org.example.utils.LogReader
import spock.lang.Specification
import spock.lang.Unroll

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class LogReaderSpec extends Specification {

    def 'Printing to System.out'() {
        given: 'A directory containing log files'
        def inputDir = 'src/test/resources/biggerUnsorted'

        when: 'Reading the files'
        new LogReader().readFilesAndWriteToStream(inputDir, System.out)

        then: 'Check the console'
        true
    }

    @Unroll
    def 'Merging/sorting #inputDir log files'() {
        given: 'A directory containing log files'
        inputDir

        when: 'Reading the files'
        def resultOutputFile = new File('src/test/resources/result/output.log')
        resultOutputFile.createNewFile()
        resultOutputFile.withOutputStream { os ->
            new LogReader().readFilesAndWriteToStream(inputDir, os)
        }

        then: 'The output contains the contents of all files sorted by timestamp'
        new File(expectedOutput).text == resultOutputFile.text
        resultOutputFile.delete()

        where:
        inputDir                            | expectedOutput
        'src/test/resources/temp'           | 'src/test/resources/expected/output1.log'
        'src/test/resources/biggerUnsorted' | 'src/test/resources/expected/output2.log'
    }

    def 'Partition files in chunks'() {
        given: 'Directory containing two log files'
        def logDir = 'src/test/resources/biggerSorted'

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
        def fileUnsorted = 'src/test/resources/biggerUnsorted/first_unsorted.log'
        when: 'Sorting'
        def sortedLines = new LogReader.Companion().sortLinesByDate(new File(fileUnsorted).readLines())
        def fileSortedDir = 'src/test/resources/biggerSorted/first_sorted.log'
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

    def 'Merge already sorted files'() {
        given: 'Directory containing three log files'
        def logDir = 'src/test/resources/biggerSorted'

        when: 'Reading the files'

        def outputFile = new File('src/test/resources/result/output.log')
        outputFile.createNewFile()
        outputFile.withOutputStream { os ->
            new LogReader().mergeFiles(logDir, os)
        }


        then: 'The output contains the contents of three files sorted by timestamp'
        def sortedLinesOutput = outputFile.readLines()
        [sortedLinesOutput.dropRight(1), sortedLinesOutput.drop(1)].transpose().every { String a, String b ->
            getDate(a) <= getDate(b)
        }
        outputFile.delete()
    }


    private static LocalDateTime getDate(String line) {
        return LocalDateTime.parse(line.substring(0, 19), DateTimeFormatter.ISO_LOCAL_DATE_TIME)
    }
}
