package com.example.demo

import org.springframework.batch.core.JobParameters
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory
import org.springframework.batch.core.launch.JobLauncher
import org.springframework.batch.item.ItemReader
import org.springframework.batch.item.file.FlatFileItemReader
import org.springframework.boot.CommandLineRunner
import org.springframework.core.io.FileSystemResource
import org.springframework.stereotype.Component
import java.io.File

@Component
class Driver(
    private val jobBuilderFactory: JobBuilderFactory,
    private val stepBuilderFactory: StepBuilderFactory,
    private val jobLauncher: JobLauncher,
) : CommandLineRunner {
    private val dirName = "data"
    override fun run(vararg args: String?) {
        val jobs = File(dirName).list { _, _ ->
            true
        }.map { filename ->
            jobBuilderFactory.get("job-$filename").start(
                stepBuilderFactory
                    .get("step1").chunk<String, String>(2)
                    .reader(reader(filename))
                    .writer { lines ->
                        lines.forEach { line -> println(line) }
                    }
                    .build()
            ).build()
        }
        for (job in jobs) {
            jobLauncher.run(job, JobParameters(emptyMap()))
        }
    }

    fun reader(filename: String): ItemReader<String> {
        val itemReader = FlatFileItemReader<String>()
        itemReader.setResource(FileSystemResource(File(dirName, filename)))
        itemReader.setLineMapper { line, _ -> line }
        return itemReader
    }
}