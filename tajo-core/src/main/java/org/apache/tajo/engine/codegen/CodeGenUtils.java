/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.engine.codegen;

import org.apache.tajo.org.objectweb.asm.ClassReader;
import org.apache.tajo.org.objectweb.asm.MethodVisitor;
import org.apache.tajo.org.objectweb.asm.Opcodes;
import org.apache.tajo.org.objectweb.asm.util.ASMifier;
import org.apache.tajo.org.objectweb.asm.util.TraceClassVisitor;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * It includes utility methods, and some of them are only used in generated code.
 * So, they appear to be not used in the project.
 */
public class CodeGenUtils {
  private static final int FLAGS = ClassReader.SKIP_DEBUG;

  public static String disassemble(byte [] bytesCode) {
    StringWriter strWriter = new StringWriter();
    PrintWriter writer = new PrintWriter(strWriter);

    ClassReader cr;
    cr = new ClassReader(bytesCode);
    cr.accept(new TraceClassVisitor(null, new ASMifier(), writer), FLAGS);

    return strWriter.toString();
  }

  @SuppressWarnings("unused")
  public static void emitPrintOut(MethodVisitor mv, String message) {
    mv.visitFieldInsn(Opcodes.GETSTATIC, "java/lang/System", "out", "Ljava/io/PrintStream;");
    mv.visitLdcInsn(message);
    mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, TajoGeneratorAdapter.getInternalName(PrintStream.class),
        "println", TajoGeneratorAdapter.getMethodDescription(void.class, new Class[]{String.class}));
  }
}
