package tajo.storage.hcfile;

import tajo.storage.hcfile.compress.Codec;

public abstract class CompressedBlock extends UpdatableBlock {

  abstract Codec getCodec();
}
