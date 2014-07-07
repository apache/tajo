/**
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

/***
 * ASM: a very small and fast Java bytecode manipulation framework
 * Copyright (c) 2000-2011 INRIA, France Telecom
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the copyright holders nor the names of its
 *    contributors may be used to endorse or promote products derived from
 *    this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.apache.tajo.org.objectweb.asm.util;

import org.apache.tajo.org.objectweb.asm.AnnotationVisitor;
import org.apache.tajo.org.objectweb.asm.Opcodes;

/**
 * An {@link org.apache.tajo.org.objectweb.asm.AnnotationVisitor} that prints the annotations it visits with a
 * {@link Printer}.
 * 
 * @author Eric Bruneton
 */
public final class TraceAnnotationVisitor extends AnnotationVisitor {

    private final Printer p;

    public TraceAnnotationVisitor(final Printer p) {
        this(null, p);
    }

    public TraceAnnotationVisitor(final AnnotationVisitor av, final Printer p) {
        super(Opcodes.ASM4, av);
        this.p = p;
    }

    @Override
    public void visit(final String name, final Object value) {
        p.visit(name, value);
        super.visit(name, value);
    }

    @Override
    public void visitEnum(final String name, final String desc,
            final String value) {
        p.visitEnum(name, desc, value);
        super.visitEnum(name, desc, value);
    }

    @Override
    public AnnotationVisitor visitAnnotation(final String name,
            final String desc) {
        Printer p = this.p.visitAnnotation(name, desc);
        AnnotationVisitor av = this.av == null ? null : this.av
                .visitAnnotation(name, desc);
        return new TraceAnnotationVisitor(av, p);
    }

    @Override
    public AnnotationVisitor visitArray(final String name) {
        Printer p = this.p.visitArray(name);
        AnnotationVisitor av = this.av == null ? null : this.av
                .visitArray(name);
        return new TraceAnnotationVisitor(av, p);
    }

    @Override
    public void visitEnd() {
        p.visitAnnotationEnd();
        super.visitEnd();
    }
}
