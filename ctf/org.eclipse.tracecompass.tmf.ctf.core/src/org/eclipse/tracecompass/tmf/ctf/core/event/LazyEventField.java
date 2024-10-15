package org.eclipse.tracecompass.tmf.ctf.core.event;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.eclipse.jdt.annotation.NonNull;
import org.eclipse.jdt.annotation.Nullable;
import org.eclipse.tracecompass.ctf.core.event.IEventDefinition;
import org.eclipse.tracecompass.ctf.core.event.types.ICompositeDefinition;
import org.eclipse.tracecompass.ctf.core.event.types.StructDeclaration;
import org.eclipse.tracecompass.ctf.core.event.types.StructDefinition;
import org.eclipse.tracecompass.tmf.core.event.ITmfEventField;
import org.eclipse.tracecompass.tmf.ctf.core.CtfConstants;

public class LazyEventField implements ITmfEventField {

    private final @NonNull String fName;
    private final @Nullable Object fValue;
    private final @NonNull IEventDefinition fDefinition;
    private Collection<? extends ITmfEventField> fFields = null;

    public LazyEventField(String name, Object value, IEventDefinition definition) {
        fName = name;
        fValue = value;
        fDefinition = definition;
    }

    @Override
    public @NonNull String getName() {
        return fName;
    }

    @Override
    public Object getValue() {
        return fValue;
    }

    @Override
    public String getFormattedValue() {
        return getValue().toString();
    }

    @Override
    public @NonNull Collection<@NonNull String> getFieldNames() {
        Collection<String> first = fDefinition.getFields().getFieldNames();
        Collection<String> second = fDefinition.getContext().getFieldNames();
        return Stream.concat(first.stream(), second.stream()).collect(Collectors.toUnmodifiableList());
    }

    @Override
    public @NonNull Collection<? extends ITmfEventField> getFields() {
        if (fFields == null) {
            /**
             * Extract the field information from the structDefinition
             * haze-inducing mess, and put them into something ITmfEventField
             * can cope with.
             */
            List<CtfTmfEventField> fields = new ArrayList<>();

            ICompositeDefinition structFields = fDefinition.getFields();
            if (structFields != null) {
                if (structFields.getFieldNames() != null) {
                    for (String fn : structFields.getFieldNames()) {
                        fields.add(CtfTmfEventField.parseField(structFields.getDefinition(fn), fn));
                    }
                }
            }
            /* Add context information as CtfTmfEventField */
            ICompositeDefinition structContext = fDefinition.getContext();
            if (structContext != null) {
                for (String contextName : structContext.getFieldNames()) {
                    /* Prefix field name */
                    String curContextName = CtfConstants.CONTEXT_FIELD_PREFIX + contextName;
                    fields.add(CtfTmfEventField.parseField(structContext.getDefinition(contextName), curContextName));
                }
            }
            fFields = fields;
        }
        return fFields;
    }

    @Override
    public ITmfEventField getField(String @NonNull... path) {
        if (path.length == 1) {
            Integer fieldPosition = ((StructDeclaration) fDefinition.getFields().getDeclaration()).getFieldPosition(path[0]);
            if (fieldPosition != -1) {
                return CtfTmfEventField.parseField(((StructDefinition) fDefinition.getFields()).getDefinition(fieldPosition), path[0]);
            }

            ICompositeDefinition structContext = fDefinition.getContext();
            fieldPosition = ((StructDeclaration) structContext.getDeclaration()).getFieldPosition(path[0]);
            if (fieldPosition != -1) {
                return CtfTmfEventField.parseField(((StructDefinition) structContext).getDefinition(fieldPosition), path[0]);
            }
            return null;
        } else if (path.length == 0) {
            return null;
        } else {
            return null;
        }
    }

}
