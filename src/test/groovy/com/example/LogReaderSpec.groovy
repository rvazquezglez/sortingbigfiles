package com.example

import org.example.utils.LogReader
import spock.lang.Specification

class LogReaderSpec extends Specification {
    def outputFileDir = 'src/test/resources/result/output.log'

    def cleanup() {
        def outputFile = new File(outputFileDir)
        outputFile.delete()
    }

    def 'Merging two small log files'() {
        given: 'Directory containing two log files'
        def logDir = "src/test/resources/temp"

        when: 'Reading the files'

        def outputFile = new File(outputFileDir)
        outputFile.createNewFile()
        outputFile.withOutputStream { os ->
            new LogReader().readFilesAndWriteToStream(logDir, os)
        }


        then: 'The output contains the contents of two files sorted by timestamp'
        new File('src/test/resources/expected/output1.log').text == outputFile.text
    }
}
