package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.PipelineContext;
import com.linkedin.thirdeye.rootcause.PipelineResult;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Pipeline for rewriting (standardizing) dimension names obtained via contribution analysis.
 * It scans any incoming DimensionEntity and matches its name against an internal list of
 * mappings. If there is a match, a new (modified) DimensionEntity is emitted, otherwise the
 * DimensionEntity passes through without modification.
 */
public class DimensionStandardizationPipeline extends Pipeline {
  private static final Logger LOG = LoggerFactory.getLogger(DimensionStandardizationPipeline.class);

  public static final String PROP_PATH = PipelineLoader.PROP_PATH;

  private final Map<String, StringMapping> dimensionMappings;

  /**
   * Constructor for dependency injection
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline config
   * @param dimensionMappings string mappings for dimension names
   */
  public DimensionStandardizationPipeline(String outputName, Set<String> inputNames, Iterable<StringMapping> dimensionMappings) {
    super(outputName, inputNames);
    this.dimensionMappings = StringMapping.toMap(dimensionMappings);
  }

  /**
   * Alternate constructor for use by PipelineLoader
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param properties configuration properties ({@code PROP_PATH})
   */
  public DimensionStandardizationPipeline(String outputName, Set<String> inputNames, Map<String, String> properties) throws IOException {
    super(outputName, inputNames);
    File csv = new File(properties.get(PROP_PATH));
    this.dimensionMappings = StringMapping.toMap(StringMappingParser.fromCsv(new FileReader(csv), 1.0d));
  }

  @Override
  public PipelineResult run(PipelineContext context) {
    Set<DimensionEntity> entities = context.filter(DimensionEntity.class);

    Set<DimensionEntity> output = new HashSet<>();
    for(DimensionEntity de : entities) {
      if(!this.dimensionMappings.containsKey(de.getName())) {
        output.add(de);
      } else {
        StringMapping sm = this.dimensionMappings.get(de.getName());
        String newName = sm.getTo();
        double newScore = de.getScore() * sm.getScore();
        DimensionEntity n = DimensionEntity.fromDimension(newScore, newName, de.getValue());
        LOG.debug("Rewriting '{}' to '{}'", de.getUrn(), n.getUrn());
        output.add(n);
      }
    }

    return new PipelineResult(context, output);
  }
}
