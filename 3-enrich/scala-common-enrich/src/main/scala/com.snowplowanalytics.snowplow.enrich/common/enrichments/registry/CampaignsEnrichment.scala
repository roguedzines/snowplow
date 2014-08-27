/*
 * Copyright (c) 2014 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics
package snowplow
package enrich
package common
package enrichments
package registry

// Java
import java.net.URI

// Maven Artifact
import org.apache.maven.artifact.versioning.DefaultArtifactVersion

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s.JValue

// Iglu
import iglu.client.SchemaKey
import iglu.client.validation.ProcessingMessageMethods._

// This project
import utils.{ConversionUtils => CU}
import utils.MapTransformer
import utils.MapTransformer._
import utils.ScalazJson4sUtils

/**
 * Companion object. Lets us create a
 * CampaignsEnrichment from a JValue
 */
object CampaignsEnrichment extends ParseableEnrichment {

  val supportedSchemaKey = SchemaKey("com.snowplowanalytics.snowplow", "campaigns", "jsonschema", "1-0-0")

  /**
   * Creates a CampaignsEnrichment instance from a JValue.
   * 
   * @param config The referer_parser enrichment JSON
   * @param schemaKey The SchemaKey provided for the enrichment
   *        Must be a supported SchemaKey for this enrichment   
   * @return a configured CampaignsEnrichment instance
   */
  def parse(config: JValue, schemaKey: SchemaKey): ValidatedNelMessage[CampaignsEnrichment] = {
    isParseable(config, schemaKey).flatMap( conf => {
      (for {
        medium    <- ScalazJson4sUtils.extract[List[String]](config, "parameters", "mktMedium")
        source    <- ScalazJson4sUtils.extract[List[String]](config, "parameters", "mktSource")
        term      <- ScalazJson4sUtils.extract[List[String]](config, "parameters", "mktTerm")
        content   <- ScalazJson4sUtils.extract[List[String]](config, "parameters", "mktContent")
        campaign  <- ScalazJson4sUtils.extract[List[String]](config, "parameters", "mktCampaign")
        enrich =  CampaignsEnrichment(medium.toSet, source.toSet, term.toSet, content.toSet, campaign.toSet)
      } yield enrich).toValidationNel
    })
  }

}

/**
 * Config for a referer_parser enrichment
 *
 * @param domains List of internal domains
 */
case class CampaignsEnrichment(
  mktMedium:   Set[String],
  mktSource:   Set[String],
  mktTerm:     Set[String],
  mktContent:  Set[String],
  mktCampaign: Set[String]
  ) extends Enrichment {

  val version = new DefaultArtifactVersion("0.1.0")

  def extractFields(uri: URI) = {

  }

}
