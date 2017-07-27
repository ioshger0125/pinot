/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.minion;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.segment.creator.SingleValueRawIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.SegmentColumnarIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.index.loader.IndexLoadingConfig;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;


public class RawIndexConverter {
  private final IndexSegment _originalIndexSegment;
  private final SegmentMetadata _originalSegmentMetadata;
  private final File _convertedIndexDir;
  private final PropertiesConfiguration _convertedProperties;

  /**
   * NOTE: original segment should be in V1 format.
   * TODO: support V3 format
   */
  public RawIndexConverter(File originalIndexDir, File convertedIndexDir) throws Exception {
    FileUtils.copyDirectory(originalIndexDir, convertedIndexDir);
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    indexLoadingConfig.setSegmentVersion(SegmentVersion.v1);
    indexLoadingConfig.setReadMode(ReadMode.mmap);
    _originalIndexSegment = ColumnarSegmentLoader.load(originalIndexDir, indexLoadingConfig);
    _originalSegmentMetadata = _originalIndexSegment.getSegmentMetadata();
    _convertedIndexDir = convertedIndexDir;
    _convertedProperties =
        new PropertiesConfiguration(new File(_convertedIndexDir, V1Constants.MetadataKeys.METADATA_FILE_NAME));
  }

  @SuppressWarnings("unchecked")
  public void convert() throws Exception {
    for (MetricFieldSpec metricFieldSpec : _originalSegmentMetadata.getSchema().getMetricFieldSpecs()) {
      if (_originalSegmentMetadata.hasDictionary(metricFieldSpec.getName())) {
        convertColumn(metricFieldSpec);
      }
    }
    List optimizations =
        _convertedProperties.getList(V1Constants.MetadataKeys.Segment.SEGMENT_OPTIMIZATIONS, new ArrayList());
    optimizations.add(V1Constants.MetadataKeys.Optimization.RAW_INDEX);
    _convertedProperties.setProperty(V1Constants.MetadataKeys.Segment.SEGMENT_OPTIMIZATIONS, optimizations);
    _convertedProperties.save();
  }

  private void convertColumn(MetricFieldSpec metricFieldSpec) throws Exception {
    String columnName = metricFieldSpec.getName();

    // Delete dictionary and existing indexes
    FileUtils.deleteQuietly(new File(_convertedIndexDir, columnName + V1Constants.Dict.FILE_EXTENTION));
    FileUtils.deleteQuietly(
        new File(_convertedIndexDir, columnName + V1Constants.Indexes.UN_SORTED_SV_FWD_IDX_FILE_EXTENTION));
    FileUtils.deleteQuietly(
        new File(_convertedIndexDir, columnName + V1Constants.Indexes.SORTED_FWD_IDX_FILE_EXTENTION));
    FileUtils.deleteQuietly(
        new File(_convertedIndexDir, columnName + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION));

    // Create the raw index
    DataSource dataSource = _originalIndexSegment.getDataSource(columnName);
    Dictionary dictionary = dataSource.getDictionary();
    FieldSpec.DataType dataType = metricFieldSpec.getDataType();
    int lengthOfLongestEntry =
        metricFieldSpec.getDataType() == FieldSpec.DataType.STRING ? metricFieldSpec.getFieldSize() : -1;
    try (SingleValueRawIndexCreator rawIndexCreator = SegmentColumnarIndexCreator.getRawIndexCreatorForColumn(
        _convertedIndexDir, columnName, dataType, _originalSegmentMetadata.getTotalDocs(), lengthOfLongestEntry)) {
      BlockSingleValIterator iterator =
          (BlockSingleValIterator) dataSource.getNextBlock().getBlockValueSet().iterator();
      int docId = 0;
      while (iterator.hasNext()) {
        int dictId = iterator.nextIntVal();
        rawIndexCreator.index(docId++, dictionary.get(dictId));
      }
    }

    // Update the segment metadata
    _convertedProperties.setProperty(
        V1Constants.MetadataKeys.Column.getKeyFor(columnName, V1Constants.MetadataKeys.Column.HAS_DICTIONARY), false);
    _convertedProperties.setProperty(
        V1Constants.MetadataKeys.Column.getKeyFor(columnName, V1Constants.MetadataKeys.Column.BITS_PER_ELEMENT), -1);
  }
}
