package uk.gov.justice.digital.hmpps.createandvaryalicenceapi.integration

import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import org.mockito.kotlin.any
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean
import org.springframework.test.context.jdbc.Sql
import software.amazon.awssdk.services.sns.model.MessageAttributeValue
import software.amazon.awssdk.services.sns.model.PublishRequest
import uk.gov.justice.digital.hmpps.createandvaryalicenceapi.entity.CommunityOffenderManager
import uk.gov.justice.digital.hmpps.createandvaryalicenceapi.integration.wiremock.extensions.DeliusMockServer
import uk.gov.justice.digital.hmpps.createandvaryalicenceapi.service.domainEvents.COM_ALLOCATED_EVENT_TYPE
import uk.gov.justice.digital.hmpps.createandvaryalicenceapi.service.domainEvents.DomainEventListener
import uk.gov.justice.digital.hmpps.createandvaryalicenceapi.service.domainEvents.HMPPSDomainEvent
import uk.gov.justice.digital.hmpps.createandvaryalicenceapi.service.domainEvents.Identifiers
import uk.gov.justice.digital.hmpps.createandvaryalicenceapi.service.domainEvents.PersonReference
import java.time.Duration
import java.util.concurrent.CompletableFuture

@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestPropertySource(properties = ["domain.event.listener.disabled=false"])
class SupportControllerComAllocationIntegrationTest : IntegrationTestBase() {

  @MockitoSpyBean
  lateinit var domainEventListener: DomainEventListener

  private val awaitAtMost30Secs
    get() = await.atMost(Duration.ofSeconds(30))

  /**
   * Reproduces: "Multiple representations of the same entity [...CommunityOffenderManager with id '...']
   * are being merged".
   *
   * This requires a CRN with two (or more) in-flight licences, both currently assigned to a COM other
   * than the one Delius returns, so that `OffenderService.updateResponsibleCom` reassigns both licences
   * to the same, already-managed `CommunityOffenderManager`. The explicit call to
   * `licenceRepository.saveAllAndFlush(offenderLicences)` forced Hibernate to `merge()` each already-managed
   * licence, cascading a merge onto the shared, managed `responsibleCom`. Merging the same licence twice in
   * one flush could load a second, detached representation of that COM, which Hibernate then refuses to
   * reconcile with the already-managed one. Removing the redundant explicit flush and relying on Hibernate's
   * dirty-checking auto-flush at transaction commit avoids the merge cascade entirely.
   */
  @Test
  @Sql(
    "classpath:test_data/seed-two-licences-same-crn.sql",
  )
  fun `Sync COM allocation for a CRN with multiple in-flight licences does not throw a Hibernate merge conflict`() {
    // Given
    val crn = "X999999"
    val staffCode = "sc_BBB"
    val userName = "BBB"
    val emailAddress = "testBBB@probation.gov.uk"
    val staffIdentifier = 126L
    val firstName = "Com"
    val lastName = "BBB"

    deliusMockServer.stubGetOffenderManager(
      crn = crn,
      staffCode = staffCode,
      userName = userName,
      emailAddress = emailAddress,
      staffIdentifier = staffIdentifier,
      firstName = firstName,
      lastName = lastName,
    )
    deliusMockServer.stubAssignDeliusRole(userName = userName.uppercase())

    val licencesBefore = testRepository.findAllLicence().filter { it.crn == crn }
    assertThat(licencesBefore).hasSize(2)

    // When
    webTestClient.put()
      .uri("/offender/sync-com/crn/$crn")
      .headers(setAuthorisation(roles = listOf("ROLE_CVL_ADMIN")))
      .exchange()
      .expectStatus().isOk

    // Then both licences are reassigned to the same, newly-allocated COM
    val licencesAfter = testRepository.findAllLicence().filter { it.crn == crn }
    assertThat(licencesAfter).hasSize(2)
    licencesAfter.forEach {
      val responsibleCom = it.responsibleCom as CommunityOffenderManager?
      assertThat(responsibleCom?.staffIdentifier).isEqualTo(staffIdentifier)
      assertThat(responsibleCom?.username).isEqualTo(userName.uppercase())
    }
  }

  @Test
  @Sql(
    "classpath:test_data/seed-two-licences-same-crn.sql",
  )
  fun `Should trigger multiple threads for REST endpoint and SQS event and fail with `() {
    // Given
    val crn = "X999999"
    val staffCode = "sc_BBB"
    val userName = "BBB"
    val emailAddress = "testBBB@probation.gov.uk"
    val staffIdentifier = 126L
    val firstName = "Com"
    val lastName = "BBB"

    deliusMockServer.stubGetOffenderManager(
      crn = crn,
      staffCode = staffCode,
      userName = userName,
      emailAddress = emailAddress,
      staffIdentifier = staffIdentifier,
      firstName = firstName,
      lastName = lastName,
    )
    deliusMockServer.stubAssignDeliusRole(userName = userName.uppercase())

    val event = buildComAllocatedEventJson(crn = crn, personUuid = null)
    val message = mapper.writeValueAsString(event)

    // When
    // -- Controller path
    val restCallFuture = CompletableFuture.runAsync {
      webTestClient.put()
        .uri("/offender/sync-com/crn/$crn")
        .headers(setAuthorisation(roles = listOf("ROLE_CVL_ADMIN")))
        .exchange()
    }

    // -- Event Path
    val eventCallFuture = CompletableFuture.runAsync {
      sendEventAndVerifyProcessed(message, event.eventType)
    }

    // Then
    CompletableFuture.allOf(restCallFuture, eventCallFuture).join()
  }

  private fun sendEventAndVerifyProcessed(message: String?, eventType: String?, eventsExpected: Int = 1) {
    sendEvent(message, eventType)

    awaitAtMost30Secs untilAsserted {
      // Two because we will also raise a licence inactivated event
      verify(domainEventListener, times(eventsExpected)).finishedEventProcessing(any())
    }
    assertThat(getNumberOfMessagesCurrentlyOnQueue()).isEqualTo(0)
  }

  private fun sendEvent(message: String?, eventType: String?) = domainEventsTopicSnsClient.publish(
    PublishRequest.builder()
      .topicArn(domainEventsTopicArn)
      .message(message)
      .messageAttributes(
        mapOf(
          "eventType" to MessageAttributeValue.builder().dataType("String").stringValue(eventType!!).build(),
        ),
      )
      .build(),
  )

  fun buildComAllocatedEventJson(
    crn: String,
    allocationId: String = "1d9ab4b2-7b80-4784-8104-f9a77fd93a31",
    occurredAt: String = "2025-04-07T11:56:00Z",
    description: String = "Person allocated event",
    eventType: String = COM_ALLOCATED_EVENT_TYPE,
    personUuid: String? = "1d9ab4b2-7b80-4784-8104-f9a77fd93a31",
    version: Int = 1,
  ): HMPPSDomainEvent = HMPPSDomainEvent(
    eventType = eventType,
    additionalInformation = mapOf("allocationId" to allocationId),
    detailUrl = personUuid?.let { "https://hmpps-workload.hmpps.service.justice.gov.uk/allocation/person/$it" },
    version = version,
    occurredAt = occurredAt,
    description = description,
    personReference = PersonReference(
      identifiers = listOf(Identifiers("CRN", crn)),
    ),
  )

  private companion object {
    @RegisterExtension
    val deliusMockServer = DeliusMockServer()
  }
}
