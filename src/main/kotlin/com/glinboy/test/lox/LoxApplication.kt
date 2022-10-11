package com.glinboy.test.lox

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.http.ResponseEntity
import org.springframework.integration.jdbc.lock.DefaultLockRepository
import org.springframework.integration.jdbc.lock.JdbcLockRegistry
import org.springframework.integration.jdbc.lock.LockRepository
import org.springframework.integration.support.locks.LockRegistry
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.RowMapper
import org.springframework.stereotype.Component
import org.springframework.stereotype.Repository
import org.springframework.util.Assert
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RestController
import java.sql.ResultSet
import java.util.*
import java.util.concurrent.TimeUnit
import javax.sql.DataSource

@SpringBootApplication
class LoxApplication

fun main(args: Array<String>) {
    runApplication<LoxApplication>(*args)
}

@Component
class Components {
    @Bean
    fun defaultLockRepository(dataSource: DataSource): DefaultLockRepository =
        DefaultLockRepository(dataSource)

    @Bean
    fun jdbcLockRegistry(repository: LockRepository): JdbcLockRegistry =
        JdbcLockRegistry(repository)
}

@RestController
class LockedResourceRestController(
    private val reservationRepository: ReservationRepository,
    private val lockRegistry: LockRegistry
) {

    @GetMapping("/")
    fun allReservation() = ResponseEntity.ok(reservationRepository.findAll())

    @GetMapping("/update/{id}/{name}/{time}")
    fun update(
        @PathVariable id: Integer,
        @PathVariable name: String,
        @PathVariable time: Long
    ): Reservation {
        val lock = lockRegistry.obtain(id.toString())
        val isLock = lock.tryLock(time, TimeUnit.MILLISECONDS)
        if (isLock) {
            try {
                doUpdate(id, name)
                Thread.sleep(time)
            } finally {
                lock.unlock()
            }
        }
        return reservationRepository.findById(id).get()
    }

    fun doUpdate(id: Integer, name: String) {
        reservationRepository.findById(id).ifPresent {
            reservationRepository.update(it.copy(name = name))
        }
    }
}

@Repository
class ReservationRepository(private val jdbcTemplate: JdbcTemplate) {
    private val rowMapper =
        RowMapper { rs: ResultSet, i: Int ->
            Reservation(
                rs.getInt("id") as Integer,
                rs.getString("name")
            )
        }

    fun findById(id: Integer): Optional<Reservation> {
        val reservations =
            jdbcTemplate.query("select * from reservation where id = ?", this.rowMapper, id)
        if (reservations.size > 0) {
            return Optional.ofNullable(reservations.iterator().next())
        }
        return Optional.empty()
    }

    fun update(reservation: Reservation): Reservation {
        Assert.isTrue(reservation.id != null && reservation.id > 0, "The id must be non-null")
        jdbcTemplate.execute(
            "update reservation set name = ? where id = ?"
        ) {
            it.setString(1, reservation.name)
            it.setInt(2, reservation.id.toInt())
            it.execute()
        }
        return findById(reservation.id).get()
    }

    fun findAll(): Collection<Reservation> {
        return jdbcTemplate.query("select * from reservation", this.rowMapper)
    }
}

data class Reservation(
    val id: Integer,
    val name: String
)